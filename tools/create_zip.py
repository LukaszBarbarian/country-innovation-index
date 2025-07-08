import os
import sys
import zipfile

source_dir = sys.argv[1]
output_path = sys.argv[2]

# Katalogi i pliki do ignorowania
ignored_dirs = {".git", ".venv", "__pycache__", "tests", "infra"}
ignored_extensions = {".pyc", ".log", ".ipynb", ".zip"}
ignored_files = {"README.md", "readme.md"}

def should_include(file_path: str):
    for part in file_path.split(os.sep):
        if part in ignored_dirs:
            return False
    filename = os.path.basename(file_path)
    if filename in ignored_files:
        return False
    _, ext = os.path.splitext(filename)
    if ext in ignored_extensions:
        return False
    return True

with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
    for root, _, files in os.walk(source_dir):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, start=source_dir)
            if should_include(rel_path):
                zipf.write(full_path, arcname=rel_path)

print(f"ZIP created at {output_path}")
