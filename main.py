import sys
import logging
from src.extractors import run_extraction
from src.transformers import DataTransformer
from src.quality import DataQualityValidator, DataLoader

# Configuração centralizada de logs do pipeline
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    """
    Orquestra o pipeline completo ponta a ponta (ETL + Data Quality):
    1. Extract: Consome APIs do IBGE e Open-Meteo e salva camada Raw (JSON).
    2. Transform: Desaninha, limpa, deduplica, cruza (JOIN) e calcula Score via DuckDB SQL.
    3. Quality & Load: Audita regras de governança e persiste em Parquet particionado (Snappy).
    """
    logger.info("=================================================================")
    logger.info("INICIANDO PIPELINE SOCIOECONÔMICO E GEOCLIMÁTICO (FT03)")
    logger.info("=================================================================")

    try:
        # 1. EXTRAÇÃO (Camada Raw)
        ibge_raw_path, clima_raw_path = run_extraction()

        # 2. TRANSFORMAÇÃO & SQL (Camada Silver/Gold)
        transformer = DataTransformer(
            ibge_path=ibge_raw_path,
            clima_path=clima_raw_path
        )
        df_transformado = transformer.transform()

        # 3. GOVERNANÇA & DATA QUALITY
        validator = DataQualityValidator(df_transformado)
        relatorio_qualidade = validator.run_all_checks(
            report_path="reports/quality_report.json"
        )

        # Trava de Segurança (Quality Gate): Só grava no Data Lake se passar em 100% dos testes
        if relatorio_qualidade["overall_status"] != "PASSED":
            logger.critical("Pipeline interrompido: Os dados não passaram no Quality Gate!")
            sys.exit(1)

        # 4. PERSISTÊNCIA OTIMIZADA (Parquet + Snappy + Particionamento ano/mes)
        DataLoader.save_partitioned_parquet(
            df=df_transformado,
            output_dir="data/output"
        )

        logger.info("=================================================================")
        logger.info("PIPELINE CONCLUÍDO COM SUCESSO!")
        logger.info("=================================================================")

    except Exception as exc:
        logger.critical(f"Erro durante a execução do pipeline: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    run_pipeline()