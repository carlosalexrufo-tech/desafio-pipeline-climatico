# Monitoramento Socioeconômico e Geoclimático — Pipeline de Engenharia de Dados

Pipeline resiliente de extração, transformação e disponibilização de dados desenvolvido em **Python** e **SQL (DuckDB)** para cálculo do **Score de Vulnerabilidade Infraestrutural & Climática** nos principais centros urbanos brasileiros.

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