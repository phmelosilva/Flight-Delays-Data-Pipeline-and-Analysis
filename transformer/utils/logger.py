import logging
import os


def get_logger(name: str = "transformer") -> logging.Logger:
    """
    Retorna um logger configurado no formato padronizado do projeto Flights.

    Dentro do Airflow:
        - Integra automaticamente com o LoggingMixin, enviando logs para a UI e o scheduler.
    Fora do Airflow:
        - Utiliza o módulo logging padrão do Python, com formatação estruturada no console.

    Args:
        name (str): Nome do logger (geralmente __name__ do módulo chamador).

    Returns:
        logging.Logger: Instância configurada do logger.
    """
    # --- Nível de log padrão (pode ser alterado via variável de ambiente LOG_LEVEL) ---
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    try:
        # Logger do Airflow (herda integração com UI e task logs).
        from airflow.utils.log.logging_mixin import LoggingMixin

        log = LoggingMixin().log
        log.name = name

        log.info("[INFO] Logger inicializado com LoggingMixin (Airflow).")

        return log

    except Exception:
        # Logger padrão para execução standalone
        log = logging.getLogger(name)

        # Evita duplicação de handlers em execuções subsequentes no mesmo kernel
        if log.hasHandlers():
            log.handlers.clear()

        # Define o nível de log global
        log.setLevel(log_level)

        # Cria handler de saída padrão (stdout)
        handler = logging.StreamHandler()

        # Define formato estruturado das mensagens de log
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        handler.setFormatter(formatter)
        log.addHandler(handler)

        # Renomeia nível WARNING para "WARN" (consistência com Airflow)
        logging.addLevelName(logging.WARNING, "WARN")

        log.info(f"[INFO] Logger inicializado no modo standalone ({log_level}).")

        return log
