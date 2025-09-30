from pathlib import Path
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log

def check_files_in_stage(stage_path: str, file_pattern: str) -> list[str]:
    """
    Verifica se existem arquivos no diretório de stage que possuem um determinado padrão.

    Args:
        stage_path (str): Caminho para o diretório de stage.
        file_pattern (str): Padrão para buscar os arquivos (ex.: '*.csv').

    Returns:
        list[str]: Lista com os caminhos completos dos arquivos encontrados.

    Raises:
        NotADirectoryError: Se o diretório de stage não for encontrado.
        FileNotFoundError: Se nenhum arquivo correspondente ao padrão for encontrado.
    """
    stage_dir = Path(stage_path)

    if not stage_dir.is_dir():
        raise NotADirectoryError(f"Diretório de stage não encontrado: '{stage_path}'")

    found_files = list(stage_dir.glob(file_pattern))

    if not found_files:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado com o padrão '{file_pattern}' em '{stage_path}'."
        )

    file_paths = [str(f) for f in found_files]
    log.info(f"Encontrados {len(file_paths)} arquivo(s): {file_paths}")

    return file_paths
