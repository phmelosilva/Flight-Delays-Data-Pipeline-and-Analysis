from pathlib import Path
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log

def check_files_in_folder(folder_path: str, file_pattern: str) -> list[str]:
    """
    Verifica se existem arquivos em um diretório que possuem um determinado padrão.

    Args:
        folder_path (str): Caminho para o diretório.
        file_pattern (str): Padrão para buscar os arquivos (ex.: '*.csv').

    Returns:
        list[str]: Lista com os caminhos completos dos arquivos encontrados.

    Raises:
        NotADirectoryError: Se o diretório não for encontrado.
        FileNotFoundError: Se nenhum arquivo correspondente ao padrão for encontrado.
    """
    folder_dir = Path(folder_path)

    if not folder_dir.is_dir():
        raise NotADirectoryError(f"Diretório não encontrado: '{folder_path}'")

    found_files = list(folder_dir.glob(file_pattern))

    if not found_files:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado com o padrão '{file_pattern}' em '{folder_path}'."
        )

    file_paths = [str(f) for f in found_files]

    log.info(f"Encontrados {len(file_paths)} arquivo(s): {file_paths}")

    return file_paths
