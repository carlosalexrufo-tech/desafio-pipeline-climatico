import logging
import sys
import duckdb
from dotenv import load_dotenv

from src.extractors import run_extraction
from src.transformers import DataTransformer
from src.quality import DataQualityValidator, DataLoader

# Configuração centralizada de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    """
    Orquestrador principal do Pipeline de Engenharia de Dados (FT03).
    Executa as etapas:
      1. Ingestão (Extract) -> APIs IBGE e Open-Meteo
      2. Processamento (Transform) -> DuckDB SQL
      3. Governança (Data Quality) -> Validação de Regras e Relatório JSON
      4. Persistência (Load) -> Parquet (Snappy) Particionado (ano/mes)
      5. Resumo Executivo -> Consulta SQL direta na camada Parquet
    """
    logger.info("=" * 65)
    logger.info("INICIANDO PIPELINE: MONITORAMENTO SOCIOECONÔMICO & CLIMÁTICO")
    logger.info("=" * 65)

    load_dotenv()

    try:
        # ------------------------------------------------------------------
        # ETAPA 1: EXTRAÇÃO (INGESTÃO RESILIENTE)
        # ------------------------------------------------------------------
        logger.info("[ETAPA 1/4] Extraindo dados das APIs públicas...")
        ibge_raw_path, clima_raw_path = run_extraction()

        # ------------------------------------------------------------------
        # ETAPA 2: TRANSFORMAÇÃO E ENRIQUECIMENTO (DUCKDB SQL)
        # ------------------------------------------------------------------
        logger.info("[ETAPA 2/4] Processando e cruzando dados via SQL (DuckDB)...")
        transformer = DataTransformer(ibge_raw_path, clima_raw_path)
        df_final = transformer.transform()

        # ------------------------------------------------------------------
        # ETAPA 3: DATA QUALITY & GOVERNANÇA
        # ------------------------------------------------------------------
        logger.info("[ETAPA 3/4] Executando Quality Gate e gerando relatório...")
        validator = DataQualityValidator(df_final)
        quality_report = validator.run_all_checks(report_path="reports/quality_report.json")

        if quality_report["overall_status"] != "PASSED":
            logger.critical("ABORTANDO PERSISTÊNCIA: Dataset reprovado no Quality Gate.")
            sys.exit(1)

        # ------------------------------------------------------------------
        # ETAPA 4: PERSISTÊNCIA (PARQUET PARTICIONADO)
        # ------------------------------------------------------------------
        logger.info("[ETAPA 4/4] Persistindo dados na camada analítica (Parquet)...")
        DataLoader.save_partitioned_parquet(df_final, output_dir="data/output")

        # ------------------------------------------------------------------
        # RESUMO EXECUTIVO DO CASE (LEITURA DA CAMADA PARQUET)
        # ------------------------------------------------------------------
        logger.info("=" * 65)
        logger.info("RESUMO EXECUTIVO - MUNICÍPIOS EM ALERTA CLIMÁTICO:")
        df_alertas = duckdb.sql("""
            SELECT DISTINCT
                municipio_nome,
                uf_sigla,
                data_referencia,
                temp_maxima_c,
                precipitacao_acumulada_mm,
                score_vulnerabilidade,
                status_alerta_climatico
            FROM read_parquet('data/output/**/*.parquet', hive_partitioning=true)
            WHERE status_alerta_climatico != 'NORMAL'
            ORDER BY data_referencia, score_vulnerabilidade DESC
        """).df()

        if df_alertas.empty:
            logger.info("Nenhum município em estado de alerta moderado ou crítico no período.")
        else:
            print("\n" + df_alertas.to_string(index=False) + "\n")

        logger.info("=" * 65)
        logger.info("PIPELINE CONCLUÍDO COM SUCESSO! TODOS OS ENTREGÁVEIS GERADOS.")
        logger.info("=" * 65)

    except Exception as e:
        logger.critical(f"Falha crítica durante a execução do pipeline: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()