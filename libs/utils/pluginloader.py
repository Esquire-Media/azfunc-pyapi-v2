from pathlib import Path
import importlib.util
import os
import sys


def load(path: str, file_mode: str = "init", depth: int = 0):
    """
    Load modules from a specified path.

    Parameters
    ----------
    path : str
        The path to the directory or file to load modules from.
    file_mode : str, optional
        The file mode to filter the modules, by default "init".
        Possible values:
        - "init": Only load modules with the filename "__init__.py".
        - "non-init": Only load modules without the filename "__init__.py".
        - "all": Load all modules regardless of the filename.
    depth : int, optional
        The depth of subdirectories to traverse.
        - If depth is 0, only modules in the specified path will be loaded.
        - If depth is -1, all modules in the subdirectories will be loaded.
        - If depth is greater than 0, modules in the specified path and up to the specified depth will be loaded.

    Returns
    -------
    list
        A list of loaded module objects.

    Notes
    -----
    This function loads Python modules from the specified path.
    It recursively traverses subdirectories based on the specified depth and filters modules based on the file mode.

    The loaded modules are returned as a list of module objects.

    Examples
    --------
    Load modules from the specified path and perform operations with them:

    >>> modules = load("/path/to/plugins", file_mode="non-init", depth=2)
    >>> for module in modules:
    ...     # Perform operations with the loaded module
    """
    
    if ".py" in path:
        parent = Path(path).parent
    else:
        parent = Path(path)
    modules = []
    for root, dirs, files in os.walk(parent):
        current_depth = root[len(path) + len(os.path.sep) :].count(os.path.sep)

        if depth != -1 and current_depth >= depth:
            dirs.clear()

        for file in files:
            if Path(root, file) != Path(path) and file.endswith(".py"):
                if (
                    (file_mode == "init" and file == "__init__.py")
                    or (file_mode == "non-init" and file != "__init__.py")
                    or (file_mode == "all")
                ):
                    module_name = ".".join(
                        Path(root, file).relative_to(Path.cwd()).parts
                    )[:-3]
                    if module_name not in sys.modules:
                        spec = importlib.util.spec_from_file_location(
                            module_name, os.path.join(root, file)
                        )
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)
                        modules.append(module)
                    else:
                        modules.append(sys.modules[module_name])
    return modules
