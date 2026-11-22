import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Any
import pandas as pd

# Configuração de logs para auditoria do pipeline
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)


class DataQualityValidator:
    """
    Módulo de Governança e Qualidade de Dados (Data Quality).
    Executa verificações automatizadas antes de liberar os dados para o Data Lake final.
    """

    # Schema na camada final
    EXPECTED_COLUMNS: List[str] = [
        "municipio_id", "municipio_nome", "uf_sigla", "regiao_nome",
        "populacao_estimada", "data_referencia", "ano", "mes",
        "temp_media_c", "temp_maxima_c", "precipitacao_acumulada_mm",
        "indice_exposicao_hidrica", "score_vulnerabilidade", "status_alerta_climatico"
    ]

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df
        self.report: Dict[str, Any] = {
            "execution_timestamp": datetime.now().isoformat(),
            "total_records": int(len(df)),
            "checks": {},
            "overall_status": "PASSED"
        }

    def check_schema(self) -> bool:
        """1. Verifica se todas as colunas obrigatórias estão presentes no DataFrame."""
        missing_cols = [col for col in self.EXPECTED_COLUMNS if col not in self.df.columns]
        passed = len(missing_cols) == 0
        
        self.report["checks"]["schema_validation"] = {
            "status": "PASSED" if passed else "FAILED",
            "expected_columns_count": len(self.EXPECTED_COLUMNS),
            "actual_columns_count": len(self.df.columns),
            "missing_columns": missing_cols
        }
        return passed

    def check_null_ratio(self, max_null_threshold: float = 0.0) -> bool:
        """2. Calcula a taxa de valores nulos (Null Ratio) por coluna."""
        null_ratios = {}
        failed_cols = []

        for col in self.EXPECTED_COLUMNS:
            if col in self.df.columns:
                ratio = round(float(self.df[col].isnull().mean()), 4)
                null_ratios[col] = ratio
                if ratio > max_null_threshold:
                    failed_cols.append(col)

        passed = len(failed_cols) == 0
        self.report["checks"]["null_ratio_validation"] = {
            "status": "PASSED" if passed else "FAILED",
            "max_allowed_ratio": max_null_threshold,
            "null_ratio_by_column": null_ratios,
            "columns_above_threshold": failed_cols
        }
        return passed

    def check_uniqueness(self) -> bool:
        """3. Verifica a unicidade da chave primária composta (municipio_id + data_referencia)."""
        duplicates_count = int(self.df.duplicated(subset=["municipio_id", "data_referencia"]).sum())
        passed = duplicates_count == 0

        self.report["checks"]["uniqueness_validation"] = {
            "status": "PASSED" if passed else "FAILED",
            "primary_key": ["municipio_id", "data_referencia"],
            "duplicate_records_found": duplicates_count
        }
        return passed

    def check_value_ranges(self) -> bool:
        """4. Testes de intervalo (Range Checks) para garantir coerência."""
        range_violations = {
            "temperatura_fora_intervalo (-10 a 55 C)": int(
                ((self.df["temp_media_c"] < -10) | (self.df["temp_media_c"] > 55)).sum()
            ),
            "precipitacao_negativa (< 0 mm)": int(
                (self.df["precipitacao_acumulada_mm"] < 0).sum()
            ),
            "populacao_invalida (<= 0)": int(
                (self.df["populacao_estimada"] <= 0).sum()
            ),
            "score_fora_escala (0 a 100)": int(
                ((self.df["score_vulnerabilidade"] < 0) | (self.df["score_vulnerabilidade"] > 100)).sum()
            )
        }

        total_violations = sum(range_violations.values())
        passed = total_violations == 0

        self.report["checks"]["range_validation"] = {
            "status": "PASSED" if passed else "FAILED",
            "total_violations": total_violations,
            "details": range_violations
        }
        return passed

    def run_all_checks(self, report_path: str = "reports/quality_report.json") -> Dict[str, Any]:
        """Executa toda a bateria de testes e exporta o relatório JSON de auditoria."""
        logger.info("Executando auditoria automatizada de Data Quality...")
        
        c1 = self.check_schema()
        c2 = self.check_null_ratio()
        c3 = self.check_uniqueness()
        c4 = self.check_value_ranges()

        if not all([c1, c2, c3, c4]):
            self.report["overall_status"] = "FAILED"
            logger.error("Falha na validação de Data Quality!")
        else:
            logger.info("Data Quality 100% APROVADO!")

        # Salva o relatório de auditoria em JSON
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(self.report, f, ensure_ascii=False, indent=2)

        logger.info(f"Relatório de auditoria salvo em: '{report_path}'")
        return self.report


class DataLoader:
    """
    Camada de Persistência (Load): Salva o dataset final validado em formato colunar
    Parquet com compressão Snappy e particionamento temporal (ano/mes).
    """

    @staticmethod
    def save_partitioned_parquet(df: pd.DataFrame, output_dir: str = "data/output") -> None:
        logger.info(f"Salvando dataset final em Parquet (Snappy) particionado em '{output_dir}'...")
        os.makedirs(output_dir, exist_ok=True)

        # Persiste usando PyArrow com particionamento estilo Hive (ano=YYYY/mes=MM)
        # Compressão Snappy 
        df.to_parquet(
            path=output_dir,
            engine="pyarrow",
            compression="snappy",
            partition_cols=["ano", "mes"],
            index=False
        )
        logger.info("Dataset persistido com sucesso no formato Parquet particionado!")


# Bloco de teste isolado
if __name__ == "__main__":
    from transformers import DataTransformer

    transformer = DataTransformer("data/raw/ibge_raw.json", "data/raw/clima_raw.json")
    df_final = transformer.transform()

    validator = DataQualityValidator(df_final)
    relatorio = validator.run_all_checks()

    if relatorio["overall_status"] == "PASSED":
        DataLoader.save_partitioned_parquet(df_final)