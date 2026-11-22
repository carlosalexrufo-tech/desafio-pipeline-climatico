# Monitoramento Socioeconômico e Geoclimático — Pipeline de Engenharia de Dados

Pipeline resiliente de extração, transformação e disponibilização de dados desenvolvido em **Python** e **SQL (DuckDB)** para cálculo do **Score de Vulnerabilidade Infraestrutural & Climática** nos principais centros urbanos brasileiros.

Link Git: https://github.com/carlosalexrufo-tech/desafio-pipeline-climatico
---

## 1. Problema de Negócio e Objetivo

Decisões de alocação de recursos públicos, manutenção de redes elétricas/hídricas e logística exigem cruzar **densidade populacional com eventos climáticos extremos** (chuvas intensas e ondas de calor). No entanto, essas informações estão dispersas em APIs públicas com estruturas distintas.

Este projeto automatiza a ingestão, o tratamento e a governança dessas fontes, entregando uma base analítica em formato colunar (**Parquet**) pronta para consumo em ferramentas de BI e Ciência de Dados.

---

## 2. Arquitetura do Projeto

```text
├── data/
│   ├── raw/                    # Camada Raw: JSONs brutos extraídos das APIs
│   └── output/                 # Camada Curated: Parquet (Snappy) particionado por ano/mes
├── reports/
│   └── quality_report.json     # Relatório automatizado de auditoria e Data Quality
├── src/
│   ├── __init__.py
│   ├── extractors.py           # Clientes HTTP resilientes (Retry + Rate-Limiting)
│   ├── transformers.py         # Desaninhamento (UNNEST) e regras de negócio em DuckDB SQL
│   └── quality.py              # Validações de schema, null ratio, unicidade e persistência
├── .env.example                # Modelo de variáveis de ambiente
├── .gitignore
├── main.py                     # Orquestrador ponta a ponta
├── README.md
├── requirements.txt
└── run.sh                      # Script de execução única
```
## 3. Fontes de Dados (APIs Públicas)
API de Localidades do IBGE (https://servicodados.ibge.gov.br/api/v1/localidades/municipios)

Domínio: Geográfico e Socioeconômico.

Dados extraídos: Código oficial do município, nome, microrregião, mesorregião, UF e macrorregião (estrutura JSON hierárquica aninhada), enriquecidos com coordenadas geográficas e população estimada.

API Open-Meteo Forecast (https://api.open-meteo.com/v1/forecast)

Domínio: Meteorológico e Climático.

Dados extraídos: Séries temporais horárias (hourly) de temperatura a 2m (temperature_2m) e precipitação pluviométrica (precipitation) em janela de 5 dias.

## 4. Transformações SQL e Governança
Utilizando o motor analítico DuckDB, o pipeline executa:

Desaninhamento (UNNEST): Explosão de listas horárias paralelas do Open-Meteo e navegação hierárquica no JSON do IBGE.

Padronização de Tipos e Datas: Conversão rigorosa via CAST (TIMESTAMP, DATE, BIGINT, DOUBLE) e extração de ano e mes.

Tratamento de Nulos: Imputação via COALESCE (zerando precipitações nulas e preenchendo temperaturas ausentes pela média municipal).

Deduplicação: Uso de Window Function (ROW_NUMBER() OVER (PARTITION BY ...)) garantindo unicidade por município e timestamp.

Cruzamento (INNER JOIN) e Enriquecimento: União via municipio_id com cálculo do Score de Vulnerabilidade (0 a 100), Índice de Exposição Hídrica e classificação de Alerta Operacional.

Data Quality Gate
Antes da escrita final, o módulo src/quality.py valida:

Integridade de Schema (14 colunas obrigatórias);

Taxa de Nulos (Null Ratio = 0.0);

Unicidade da chave composta (municipio_id, data_referencia);

Testes de intervalo físico (temperatura, precipitação, população e escala do score).

## 5. Como Executar o Projeto
Pré-requisitos
Python 3.10 ou superior instalado.

Variáveis de Ambiente
Crie um arquivo .env na raiz do projeto baseado no .env.example:

Snippet de código
IBGE_API_URL=[https://servicodados.ibge.gov.br/api/v1/localidades/municipios](https://servicodados.ibge.gov.br/api/v1/localidades/municipios)
OPEN_METEO_API_URL=[https://api.open-meteo.com/v1/forecast](https://api.open-meteo.com/v1/forecast)
Passo a Passo
Clone o repositório:

Bash
git clone [https://github.com/carlosalexrufo-tech/desafio-pipeline-climatico.git](https://github.com/carlosalexrufo-tech/desafio-pipeline-climatico.git)
cd desafio-pipeline-climatico
Configure as variáveis de ambiente:

Bash
cp .env.example .env
Instale as dependências e execute o pipeline completo:

Bash
pip install -r requirements.txt
python main.py
