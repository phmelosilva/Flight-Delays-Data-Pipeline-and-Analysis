import logging
import os


def get_logger(name: str = "transformer"):
    """
    Retorna um logger configurado para funcionar tanto dentro quanto fora do Airflow.
    
    - Dentro do Airflow: integra com o scheduler/webserver via LoggingMixin.
    - Fora do Airflow: usa logging padrão do Python, com formatação estruturada.
    
    Args:
        name (str): nome do logger, normalmente __name__ do módulo chamador.

    Returns:
        logging.Logger: instância configurada de logger.
    """
    # Tentativa de importar o logger nativo do Airflow
    try:
        from airflow.utils.log.logging_mixin import LoggingMixin
        log = LoggingMixin().log
        log.name = name
        log.debug("Logger inicializado com LoggingMixin (modo Airflow).")
        return log

    except Exception:
        # Caso Airflow não esteja disponível (ex: em notebooks)
        log = logging.getLogger(name)

        # Evita reconfigurar loggers existentes
        if not log.handlers:
            log.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            handler.setFormatter(formatter)
            log.addHandler(handler)

        log.debug("Logger inicializado no modo standalone.")
        return log
