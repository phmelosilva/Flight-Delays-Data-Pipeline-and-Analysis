from pathlib import Path
import sys


def init_project(levels_up: int = 2, verbose: bool = True):
    """
    Inicializa o contexto do projeto adicionando o diretório raiz ao sys.path.
    
    Args:
        levels_up (int): Níveis acima do arquivo atual para chegar à raiz do projeto.
        verbose (bool): Exibe mensagem de confirmação se True.
    """
    root = Path(__file__).resolve().parents[levels_up]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
        if verbose:
            print(f"[INFO] Raiz adicionado ao sys.path: {root}")
    else:
        if verbose:
            print(f"[INFO] Raiz já presente no sys.path: {root}")
    return root
