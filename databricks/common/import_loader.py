import types
import sys

def load_module_from_dbfs(module_name: str, dbfs_path: str):
    code = dbutils.fs.head(dbfs_path)
    module = types.ModuleType(module_name)
    exec(code, module.__dict__)
    sys.modules[module_name] = module
    return module
