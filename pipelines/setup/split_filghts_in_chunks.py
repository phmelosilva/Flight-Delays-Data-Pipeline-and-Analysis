import logging
import pandas as pd
import numpy as np
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


SOURCE_FILE_NAME = "flights.csv"
NUM_CHUNKS = 10


def split_main_file():
    """
    Divide um arquivo flights.csv em múltiplos chunks de tamanho aproximadamente igual.

    Raises:
        FileNotFoundError: Se o arquivo de origem não existir.
        ValueError: Se o arquivo estiver vazio.
        Exception: Para erros inesperados.
    """

    script_dir = Path(__file__).resolve().parent
    source_file = script_dir / SOURCE_FILE_NAME

    if not source_file.exists():
        raise FileNotFoundError(f"Arquivo não encontrado no diretório: '{source_file}'")

    try:
        df = pd.read_csv(source_file)
        if df.empty:
            raise ValueError(f"O arquivo '{source_file}' está vazio.")
        
        num_chunks = min(NUM_CHUNKS, len(df))
        chunks = np.array_split(df, num_chunks)
        
        log.info(f"Dividindo em {num_chunks} partes aproximadamente iguais...")

        for i, chunk_df in enumerate(chunks, start=1):
            chunk_name = f"{source_file.stem}_part_{i:02d}{source_file.suffix}"
            chunk_path = source_file.parent / chunk_name

            log.info(f"Salvando chunk: '{chunk_name}' ({len(chunk_df)} linhas)")
            chunk_df.to_csv(chunk_path, index=False)

        log.info("Divisão do arquivo concluída com sucesso.")

    except Exception as e:
        log.error(f"Erro: {e}")
        raise


if __name__ == '__main__':
    log.info("Iniciando a divisão de arquivos.")
    try:
        split_main_file()
        log.info("Divisão finalizado com sucesso.")
    except Exception:
        log.error("Divisão finalizado com erro.")
