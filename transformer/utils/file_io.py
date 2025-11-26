from datetime import datetime
from pathlib import Path
from collections import defaultdict
from pyspark.sql import SparkSession
from transformer.utils.logger import get_logger

log = get_logger("file_io")


def find_partition(base_path: str, mode: str = "latest", date_str: str | None = None) -> str:
    """
    Localiza e retorna o nome da partição de data a ser utilizada (ex: '2025-01-01').

    Se mode="latest", busca automaticamente a pasta mais recente no formato YYYY-MM-DD.
    Se mode="date", valida e retorna o valor informado em `date_str`.

    Args:
        base_path (str): Caminho base da camada (ex: '/opt/airflow/data-layer/silver').
        mode (str): "latest" ou "date".
        date_str (str, optional): Data específica (YYYY-MM-DD).

    Returns:
        str: Nome da partição encontrada.

    Raises:
        ValueError: Se o modo for inválido ou o formato da data for incorreto.
        FileNotFoundError: Se nenhuma partição válida for encontrada.
    """
    base = Path(base_path)

    # Função auxiliar para validar formato de data YYYY-MM-DD
    def _is_yyyy_mm_dd(name: str) -> bool:
        try:
            datetime.strptime(name, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    # Caso o modo seja "date", valida data primeiro
    if mode == "date":
        if not date_str:
            raise ValueError("[ERROR] Uma data deve ser informada quando mode='date'.")
        return date_str

    # Verificação de modo válido
    if mode != "latest":
        raise ValueError(f"[ERROR] Modo inválido: {mode}. Use 'latest' ou 'date'.")

    # Busca todas as subpastas com formato de data válido
    candidates = [
        p.name for p in base.iterdir()
        if p.is_dir() and _is_yyyy_mm_dd(p.name)
    ]

    if not candidates:
        raise FileNotFoundError(f"[ERROR] Nenhuma partição encontrada em {base_path}.")

    # Seleciona a partição mais recente
    latest_partition = sorted(candidates)[-1]
    log.info(f"[INFO] Partição selecionada: {latest_partition}")

    return latest_partition


def delete_files(spark: SparkSession, files_to_delete: list[str]) -> None:
    """
    Deleta arquivos de um determinado local via Hadoop FileSystem.

    Args:
        spark (SparkSession): Sessão Spark ativa.
        files_to_delete (list[str]): Lista de caminhos a serem removidos.

    Raises:
        IOError: Se algum arquivo não puder ser removido.
        Exception: Para falhas inesperadas na operação.
    """
    if not files_to_delete:
        log.warning("[WARN] Nenhum arquivo para deletar.")
        return

    # Criação de referência ao FileSystem Hadoop
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    HPath = spark._jvm.org.apache.hadoop.fs.Path

    log.info(f"[INFO] Deletando {len(files_to_delete)} arquivo(s).")

    # Itera sobre os arquivos e remove cada um
    for file_path in files_to_delete:
        path_to_delete = HPath(file_path)
        try:
            if fs.exists(path_to_delete):
                # Deleta arquivo ou diretório (recursivo)
                if fs.delete(path_to_delete, True):
                    log.info(f"[INFO] '{file_path}' deletado com sucesso.")
                else:
                    raise IOError(f"[ERROR] Falha ao deletar '{file_path}'.")
            else:
                log.warning(f"[WARN] Arquivo '{file_path}' não encontrado. Ignorando.")
        except Exception as e:
            log.error(f"[ERROR] Erro ao deletar '{file_path}': {e}")
            raise

    log.info("[INFO] Deleção concluída.")


def move_files(
    spark: SparkSession,
    source_files: list[str],
    base_dest_path: str,
    processing_date: str
) -> None:
    """
    Move arquivos entre camadas via FileSystem Hadoop, criando subdiretórios por extensão.

    Exemplo de estrutura de destino: /data-layer/bronze/2025-01-01/PARQUET/

    Args:
        spark (SparkSession): Sessão Spark ativa.
        source_files (list[str]): Caminhos completos dos arquivos de origem.
        base_dest_path (str): Caminho base do diretório de destino.
        processing_date (str): Data da partição (ex: '2025-11-08').

    Raises:
        ValueError: Se a lista de arquivos estiver vazia.
        IOError: Se ocorrer erro ao criar diretórios ou mover arquivos.
    """
    if not source_files:
        raise ValueError("[ERROR] Nenhum caminho fornecido para movimentação.")

    # Acesso ao FileSystem Hadoop
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    HPath = spark._jvm.org.apache.hadoop.fs.Path

    # Agrupa arquivos por tipo de extensão
    destinations = defaultdict(list)
    for file_path in source_files:
        py_path = Path(file_path)
        extension = py_path.suffix.lstrip(".").upper() if py_path.suffix else "UNKNOWN"

        # Diretórios sem extensão são tratados como PARQUET
        if extension == "UNKNOWN" and py_path.is_dir():
            extension = "PARQUET"

        dest_dir = f"{base_dest_path}/{processing_date}/{extension}"
        destinations[dest_dir].append(file_path)

    log.info(f"[INFO] Movendo arquivos para '{base_dest_path}'.")

    # Criação dos diretórios de destino (caso não existam)
    for dest_dir_str in destinations.keys():
        dest_path = HPath(dest_dir_str)
        try:
            if not fs.exists(dest_path):
                if fs.mkdirs(dest_path):
                    log.info(f"[INFO] Diretório criado: {dest_dir_str}")
                else:
                    raise IOError(f"[ERROR] Falha ao criar diretório: {dest_dir_str}")
        except Exception as e:
            log.error(f"[ERROR] Erro ao criar diretório '{dest_dir_str}': {e}")
            raise IOError(f"[ERROR] Erro ao criar diretório '{dest_dir_str}': {e}") from e

    # Movimentação dos arquivos de origem para o destino
    for dest_dir_str, files_to_move in destinations.items():
        for file_path in files_to_move:
            source_path = HPath(file_path)
            target_path = HPath(f"{dest_dir_str}/{Path(file_path).name}")
            try:
                if fs.rename(source_path, target_path):
                    log.info(f"[INFO] '{source_path.getName()}' movido para '{target_path}'.")
                else:
                    raise IOError(f"[ERROR] Falha ao mover '{file_path}' para '{target_path}'.")
            except Exception as e:
                log.error(f"[ERROR] Erro ao mover '{file_path}': {e}")
                raise IOError(f"[ERROR] Erro ao mover '{file_path}': {e}") from e

    log.info("[INFO] Movimentação concluída com sucesso.")


def check_files_in_folder(folder_path: str, file_pattern: str) -> list[str]:
    """
    Verifica e retorna os arquivos que correspondem a um padrão dentro de um diretório.

    Args:
        folder_path (str): Caminho do diretório.
        file_pattern (str): Padrão de correspondência (ex: '*.csv').

    Returns:
        list[str]: Caminhos completos dos arquivos encontrados.

    Raises:
        NotADirectoryError: Se o diretório não existir.
        FileNotFoundError: Se nenhum arquivo correspondente for encontrado.
    """
    folder_dir = Path(folder_path)

    # Verifica se o caminho informado
    if not folder_dir.is_dir():
        raise NotADirectoryError(f"[ERROR] Diretório não encontrado: '{folder_path}'")

    # Busca os arquivos que correspondem ao padrão informado
    found_files = list(folder_dir.glob(file_pattern))
    if not found_files:
        raise FileNotFoundError(
            f"[ERROR] Nenhum arquivo encontrado com o padrão '{file_pattern}' em '{folder_path}'."
        )

    # Retorna lista de caminhos completos
    file_paths = [str(f) for f in found_files]
    log.info(f"[INFO] Encontrados {len(file_paths)} arquivo(s).")

    return file_paths
