import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
sys.path.insert(0, SRC_PATH)

print(f"[conftest.py] PROJECT_ROOT: {PROJECT_ROOT}")
print(f"[conftest.py] SRC_PATH: {SRC_PATH}")
print(f"[conftest.py] sys.path: {sys.path[:5]}")  # tylko pierwsze 5 pozycji dla czytelności
