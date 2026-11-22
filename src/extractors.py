import os
import time
import json
import logging
from typing import Dict, List, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# ==============================================================================
# ETAPA 1: CONFIGURAÇÕES INICIAIS E SEGURANÇA
# ==============================================================================
# O load_dotenv() lê o arquivo oculto '.env' e carrega os endereços das APIs.
# Não deixar configurações ou chaves expostas diretamente no código-fonte.

load_dotenv()

# Configuração do sistema de Logs (mensagens organizadas no terminal).
# 'logging' para registrar a data, hora e o nível de gravidade (INFO, ERROR) de cada etapa.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)

# ==============================================================================
# ETAPA 2: CATÁLOGO DE REFERÊNCIA GEOGRÁFICA (REGRA DE NEGÓCIO)
# ==============================================================================
# Decisão de Arquitetura: A API de Localidades do IBGE retorna o código oficial
# do município (ex: 2927408 para Salvador), mas não traz latitude e longitude.
# Como a API de Clima (Open-Meteo) exige coordenadas geográficas, mapeamos
# 12 capitais estratégicas brasileiras para garantir o cruzamento perfeito (JOIN).
MUNICIPIOS_ESTRATEGICOS: Dict[int, Dict[str, Any]] = {
    2927408: {"lat": -12.9714, "lon": -38.5014, "populacao_estimada": 2418005},  # Salvador - BA
    3550308: {"lat": -23.5505, "lon": -46.6333, "populacao_estimada": 11451245}, # São Paulo - SP
    3304557: {"lat": -22.9068, "lon": -43.1729, "populacao_estimada": 6211423},  # Rio de Janeiro - RJ
    5300108: {"lat": -15.7975, "lon": -47.8919, "populacao_estimada": 2817068},  # Brasília - DF
    2304400: {"lat": -3.7172,  "lon": -38.5433, "populacao_estimada": 2428678},  # Fortaleza - CE
    3106200: {"lat": -19.9167, "lon": -43.9345, "populacao_estimada": 2315560},  # Belo Horizonte - MG
    1302603: {"lat": -3.1190,  "lon": -60.0217, "populacao_estimada": 2063547},  # Manaus - AM
    4106902: {"lat": -25.4284, "lon": -49.2733, "populacao_estimada": 1773733},  # Curitiba - PR
    2611606: {"lat": -8.0476,  "lon": -34.8770, "populacao_estimada": 1488920},  # Recife - PE
    4314902: {"lat": -30.0346, "lon": -51.2177, "populacao_estimada": 1332570},  # Porto Alegre - RS
    1501402: {"lat": -1.4558,  "lon": -48.4902, "populacao_estimada": 1303389},  # Belém - PA
    5208707: {"lat": -16.6869, "lon": -49.2648, "populacao_estimada": 1437237},  # Goiânia - GO
}


# ==============================================================================
# ETAPA 3: CLIENTE HTTP RESILIENTE (POO + TRATAMENTO DE FALHAS)
# ==============================================================================
class BaseAPIClient:
    """
    Classe base responsável por fazer requisições na internet com segurança.
    Implementa 'Rate-Limiting' (controle de ritmo) e 'Retries' (retentativas).
    """

    def __init__(self, retries: int = 3, backoff_factor: float = 1.0) -> None:
        # Cria uma sessão HTTP (reaproveita a conexão de rede, deixando mais rápido)
        self.session = requests.Session()
        
        # ESTRATÉGIA DE RETENTATIVA (Exponential Backoff):
        # Se a API retornar erro 429 (Muitas requisições) ou 500/502/503/504 (Servidor instável),
        # Tentativas 3 vezes, esperando 1s, depois 2s, depois 4s.
        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def fetch_data(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Executa a chamada na URL e devolve os dados convertidos em JSON (dicionário/lista).
        """
        try:
            # RATE-LIMITING: Pausa de 0.25 segundos entre uma chamada e outra para não
            # bombardear o servidor público e evitar bloqueio do nosso IP.
            time.sleep(0.25)
            
            response = self.session.get(url, params=params, timeout=15)
            
            # Verifica se o status HTTP foi sucesso (200). Se for erro (404, 500), gera exceção.
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as err:
            logger.error(f"Falha na requisição HTTP para {url}: {err}")
            raise


# ==============================================================================
# ETAPA 4: EXTRATORES ESPECÍFICOS DE CADA API (HERANÇA EM POO)
# ==============================================================================
class IBGEExtractor(BaseAPIClient):
    """
    Herda a inteligência de rede da 'BaseAPIClient' para consumir a API do IBGE.
    """

    def __init__(self) -> None:
        super().__init__()
        # Lê a URL do arquivo .env (com um valor padrão caso o .env falhe)
        self.base_url = os.getenv(
            "IBGE_API_URL",
            "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
        )

    def extract(self) -> List[Dict[str, Any]]:
        """
        Busca todos os municípios do Brasil no IBGE e filtra os 12 estratégicos,
        mantendo toda a árvore JSON aninhada original (microrregião, mesorregião, UF).
        """
        logger.info("Iniciando extração da API do IBGE...")
        todos_municipios = self.fetch_data(self.base_url)
        
        filtrados = []
        for mun in todos_municipios:
            mun_id = mun.get("id")
            if mun_id in MUNICIPIOS_ESTRATEGICOS:
                # Anexa lat, lon e população ao objeto do município
                mun["metadados_geo"] = MUNICIPIOS_ESTRATEGICOS[mun_id]
                filtrados.append(mun)
                
        logger.info(f"Extração IBGE concluída: {len(filtrados)} municípios selecionados.")
        return filtrados


class OpenMeteoExtractor(BaseAPIClient):
    """
    Herda a inteligência de rede da 'BaseAPIClient' para consumir a API Open-Meteo.
    """

    def __init__(self) -> None:
        super().__init__()
        self.base_url = os.getenv(
            "OPEN_METEO_API_URL",
            "https://api.open-meteo.com/v1/forecast"
        )

    def extract_for_municipalities(self, municipios: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Para cada município extraído do IBGE, consulta a previsão climática de 5 dias.
        """
        logger.info("Iniciando extração da API Open-Meteo (Clima e Precipitação)...")
        resultados_clima = []
        
        for mun in municipios:
            mun_id = mun["id"]
            geo = mun["metadados_geo"]
            
            # Parâmetros enviados na URL da API Open-Meteo
            params = {
                "latitude": geo["lat"],
                "longitude": geo["lon"],
                "hourly": "temperature_2m,precipitation", # Temperatura (2m do solo) e chuva (mm)
                "forecast_days": 5,                       # Janela de 5 dias (120 horas)
                "timezone": "America/Sao_Paulo"
            }
            
            clima_json = self.fetch_data(self.base_url, params=params)
            
            # CHAVE DE LIGAÇÃO (Foreign Key): Guardamos o ID do IBGE dentro do JSON de clima
            # para podermos fazer o cruzamento (JOIN) no SQL na próxima etapa!
            clima_json["municipio_id"] = mun_id
            resultados_clima.append(clima_json)
            
        logger.info(f"Extração Open-Meteo concluída: {len(resultados_clima)} registros climáticos obtidos.")
        return resultados_clima


# ==============================================================================
# ETAPA 5: ORQUESTRAÇÃO DA CAMADA RAW (DADOS BRUTOS)
# ==============================================================================
def run_extraction() -> tuple[str, str]:
    """
    Executa as duas extrações e salva os arquivos JSON brutos na pasta 'data/raw/'.
    Salvar a camada Raw é uma boa prática de Data Lake para auditoria e reprocessamento.
    """
    os.makedirs("data/raw", exist_ok=True)
    
    # 1. Extrai dados do IBGE
    ibge_ext = IBGEExtractor()
    dados_ibge = ibge_ext.extract()
    
    # 2. Extrai dados de Clima usando as coordenadas dos municípios do IBGE
    meteo_ext = OpenMeteoExtractor()
    dados_clima = meteo_ext.extract_for_municipalities(dados_ibge)
    
    # 3. Persiste os arquivos brutos (Raw)
    ibge_path = "data/raw/ibge_raw.json"
    clima_path = "data/raw/clima_raw.json"
    
    with open(ibge_path, "w", encoding="utf-8") as f:
        json.dump(dados_ibge, f, ensure_ascii=False, indent=2)
        
    with open(clima_path, "w", encoding="utf-8") as f:
        json.dump(dados_clima, f, ensure_ascii=False, indent=2)
        
    logger.info(f"Arquivos brutos salvos com sucesso: '{ibge_path}' e '{clima_path}'.")
    return ibge_path, clima_path


# Bloco de execução direta (permite testar este arquivo isoladamente)
if __name__ == "__main__":
    run_extraction()