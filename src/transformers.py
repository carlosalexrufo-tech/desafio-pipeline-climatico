import logging
import duckdb
import pandas as pd

# Configuração de logs para acompanhar cada etapa da transformação
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Camada de Transformação (Silver/Gold) utilizando SQL via DuckDB.
    Responsável por desaninhar os JSONs brutos, limpar, deduplicar, cruzar as fontes
    e calcular o Score de Vulnerabilidade Infraestrutural & Climática.
    """

    def __init__(self, ibge_path: str, clima_path: str) -> None:
        self.ibge_path = ibge_path
        self.clima_path = clima_path
        # Cria um banco de dados SQL analítico ultrarrápido direto na memória RAM
        self.con = duckdb.connect(database=":memory:")

    def transform(self) -> pd.DataFrame:
        """
        Executa o pipeline SQL estruturado em CTEs (Common Table Expressions)
        e retorna um DataFrame Pandas limpo.
        """
        logger.info("Iniciando pipeline de transformação SQL com DuckDB...")

        # Decisão Técnica: CTEs (blocos WITH) para dividir a query em etapas.
        query = f"""
        WITH ibge_desaninhado AS (
            -- =========================================================================
            -- ETAPA 1: DESANINHAMENTO HIERÁRQUICO (IBGE) & PADRONIZAÇÃO DE TIPOS
            -- =========================================================================
            -- O JSON do IBGE traz dados aninhados em vários níveis (microrregiao -> UF -> regiao).
            -- Campos para colunas planas e formatação dos tipos com CAST.
            SELECT 
                CAST(id AS BIGINT) AS municipio_id,
                TRIM(nome) AS municipio_nome,
                microrregiao.mesorregiao.UF.sigla AS uf_sigla,
                microrregiao.mesorregiao.UF.nome AS uf_nome,
                microrregiao.mesorregiao.UF.regiao.nome AS regiao_nome,
                CAST(metadados_geo.lat AS DOUBLE) AS latitude,
                CAST(metadados_geo.lon AS DOUBLE) AS longitude,
                CAST(metadados_geo.populacao_estimada AS BIGINT) AS populacao_estimada
            FROM read_json_auto('{self.ibge_path}')
        ),

        clima_explodido AS (
            -- =========================================================================
            -- ETAPA 2: DESANINHAMENTO DE COLEÇÕES / ARRAYS (UNNEST / EXPLODE)
            -- =========================================================================
            -- A API Open-Meteo entrega 120 horas dentro de listas paralelas ('hourly').
            -- Função UNNEST do SQL trasnformar listas em linhas individuais de tabela.
            SELECT 
                CAST(municipio_id AS BIGINT) AS municipio_id,
                UNNEST(hourly.time) AS data_hora_str,
                UNNEST(hourly.temperature_2m) AS temperatura_c,
                UNNEST(hourly.precipitation) AS precipitacao_mm
            FROM read_json_auto('{self.clima_path}')
        ),

        clima_limpo_deduplicado AS (
            -- =========================================================================
            -- ETAPA 3: PADRONIZAÇÃO DE DATAS, TRATAMENTO DE NULOS E DEDUPLICAÇÃO
            -- =========================================================================
            SELECT 
                municipio_id,
                CAST(data_hora_str AS TIMESTAMP) AS data_hora,
                CAST(CAST(data_hora_str AS TIMESTAMP) AS DATE) AS data_referencia,
                
                -- Tratamento de Nulos (COALESCE): Se chuva vier nula, assume 0.0 mm
                COALESCE(CAST(precipitacao_mm AS DOUBLE), 0.0) AS precipitacao_mm,
                
                -- Tratamento de Nulos: Se temperatura vier nula, imputa a média do município
                COALESCE(
                    CAST(temperatura_c AS DOUBLE),
                    AVG(CAST(temperatura_c AS DOUBLE)) OVER (PARTITION BY municipio_id)
                ) AS temperatura_c,
                
                -- Deduplicação via Window Function: numera registros repetidos de mesma data/hora
                ROW_NUMBER() OVER (
                    PARTITION BY municipio_id, data_hora_str 
                    ORDER BY data_hora_str
                ) AS linha_num
            FROM clima_explodido
            WHERE data_hora_str IS NOT NULL
        ),

        clima_agregado_diario AS (
            -- =========================================================================
            -- ETAPA 4: AGREGAÇÃO TEMPORAL (GRANULARIDADE DIÁRIA)
            -- =========================================================================
            -- Consolida as medições horárias em indicadores diários e extrai ano/mês
            -- chave de particionamento no arquivo Parquet final.
            SELECT 
                municipio_id,
                data_referencia,
                CAST(EXTRACT(YEAR FROM data_referencia) AS INTEGER) AS ano,
                CAST(EXTRACT(MONTH FROM data_referencia) AS INTEGER) AS mes,
                ROUND(AVG(temperatura_c), 2) AS temp_media_c,
                ROUND(MAX(temperatura_c), 2) AS temp_maxima_c,
                ROUND(SUM(precipitacao_mm), 2) AS precipitacao_acumulada_mm
            FROM clima_limpo_deduplicado
            WHERE linha_num = 1 -- Filtra apenas registros únicos (remove duplicatas)
            GROUP BY municipio_id, data_referencia
        )

        -- =============================================================================
        -- ETAPA 5: CRUZAMENTO DE FONTES (JOIN) & ENRIQUECIMENTO DE MÉTRICAS (INSIGHT)
        -- =============================================================================
        -- Une a dimensão demográfica (IBGE) com os fatos climáticos (Open-Meteo)
        -- e calcula o Score de Vulnerabilidade Infraestrutural & Climática (0 a 100).
        SELECT 
            i.municipio_id,
            i.municipio_nome,
            i.uf_sigla,
            i.regiao_nome,
            i.populacao_estimada,
            c.data_referencia,
            c.ano,
            c.mes,
            c.temp_media_c,
            c.temp_maxima_c,
            c.precipitacao_acumulada_mm,
            
            -- Métrica Enriquecida 1: Índice de Exposição Hídrica (População x Chuva em milhões)
            ROUND((i.populacao_estimada * c.precipitacao_acumulada_mm) / 1000000.0, 4) AS indice_exposicao_hidrica,
            
            -- Métrica Enriquecida 2: Score de Vulnerabilidade Infraestrutural & Climática (0 a 100)
            -- Combina peso demográfico (até 40 pts), volume de chuva (até 40 pts) e calor extremo (até 20 pts)
            ROUND(
                LEAST(40.0, (i.populacao_estimada / 12000000.0) * 40.0) +
                LEAST(40.0, (c.precipitacao_acumulada_mm / 50.0) * 40.0) +
                CASE 
                    WHEN c.temp_maxima_c >= 32.0 THEN 20.0 
                    ELSE (c.temp_maxima_c / 32.0) * 10.0 
                END
            , 2) AS score_vulnerabilidade,

            -- Métrica Enriquecida 3: Classificação de Alerta para Tomada de Decisão
            CASE 
                WHEN c.precipitacao_acumulada_mm >= 30.0 AND i.populacao_estimada >= 2000000 
                    THEN 'CRÍTICO - RISCO INFRAESTRUTURAL'
                WHEN c.precipitacao_acumulada_mm >= 15.0 OR c.temp_maxima_c >= 34.0 
                    THEN 'ALERTA MODERADO'
                ELSE 'NORMAL'
            END AS status_alerta_climatico

        FROM ibge_desaninhado i
        INNER JOIN clima_agregado_diario c 
            ON i.municipio_id = c.municipio_id
        ORDER BY c.data_referencia ASC, score_vulnerabilidade DESC
        """

        df_resultado = self.con.execute(query).df()
        logger.info(f"Transformação concluída com sucesso! Total de linhas: {len(df_resultado)}")
        return df_resultado


# Bloco de teste isolado para estudo e validação
if __name__ == "__main__":
    transformer = DataTransformer(
        ibge_path="data/raw/ibge_raw.json",
        clima_path="data/raw/clima_raw.json"
    )
    df = transformer.transform()
    print("\n--- TOP 10 MUNICÍPIOS POR SCORE DE VULNERABILIDADE ---")
    colunas_exibicao = [
        "municipio_nome", "uf_sigla", "data_referencia", 
        "precipitacao_acumulada_mm", "score_vulnerabilidade", "status_alerta_climatico"
    ]
    print(df[colunas_exibicao].head(10).to_string(index=False))