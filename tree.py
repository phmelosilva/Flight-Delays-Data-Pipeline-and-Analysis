import os

def print_tree(startpath, prefix="", ignore={".git", "__pycache__", ".venv", "dbdata"}):
    files = [f for f in sorted(os.listdir(startpath)) if f not in ignore]
    
    for i, f in enumerate(files):
        path = os.path.join(startpath, f)
        connector = "└── " if i == len(files)-1 else "├── "
        print(prefix + connector + f)
        if os.path.isdir(path):
            extension = "    " if i == len(files)-1 else "│   "
            print_tree(path, prefix + extension, ignore)

print_tree(".")
