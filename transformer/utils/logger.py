import logging
import os


def get_logger(name: str = "transformer") -> logging.Logger:
    """
    Retorna um logger configurado no formato padronizado para o projeto.

    Comportamento:
        - Se estiver rodando sob Airflow, usa LoggingMixin para integração com UI.
        - Fora do Airflow, cria logger padrão com formatação consistente.

    Args:
        name (str): Nome do logger a ser retornado.

    Returns:
        logging.Logger: Logger configurado.
    """
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    try:
        # Tenta integrar com o logger nativo do Airflow (UI, scheduler, task logs)
        from airflow.utils.log.logging_mixin import LoggingMixin

        log = LoggingMixin().log
        log.name = name
        return log

    except Exception:
        log = logging.getLogger(name)

        # Impede duplicação de handlers em execuções consecutivas (ex.: notebooks)
        if log.hasHandlers():
            log.handlers.clear()

        log.setLevel(log_level)

        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        handler.setFormatter(formatter)
        log.addHandler(handler)

        # Padroniza o nível 'WARNING' para 'WARN', em linha com Airflow e Spark
        logging.addLevelName(logging.WARNING, "WARN")

        return log
