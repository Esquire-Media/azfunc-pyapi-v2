from azure.functions import AuthLevel, FunctionApp, DecoratorApi
from deployment import BLUEPRINTS

try:
    from development import BLUEPRINTS as DEV_BLUEPRINTS
except:
    DEV_BLUEPRINTS = []
import importlib.util, inspect, os


def find_blueprints(path):
    """
    Find blueprints in the specified path.

    Parameters
    ----------
    path : str
        The path to search for blueprints.

    Returns
    -------
    List[Blueprint]
        The list of found blueprints.

    Notes
    -----
    This static method finds blueprints in the specified path.
    It scans the path for Python files and processes them as blueprints.

    Steps:
    1. Initialize variables for recursive and single-file search.
    2. Check if the path ends with "/*" to determine if it is recursive.
    3. Check if the path ends with "/" to determine if it is a single file.
    4. Define a function to process a single file.
    5. If it is a single file, process it.
    6. If it is recursive, scan the path for Python files and process each file.
    7. Return the list of registered blueprints.

    The find_blueprints method can be used to find and retrieve blueprints from a specific path.
    """
    blueprints = []
    recursive = False
    single_file = False

    # Check if path ends with /*
    if path.endswith("/*"):
        path = path[:-2]  # Remove /* from the end
        recursive = True
    elif path.endswith("/"):
        path = path[:-1]  # Remove / from the end
    else:
        single_file = True  # If it doesn't end with / or /*, assume it's a single file

    # Function to process a single file
    def process_file(file_path):
        # Check if the file exists
        if not os.path.exists(file_path):
            return

        # Load the module
        spec = importlib.util.spec_from_file_location(
            os.path.basename(file_path)[:-3], file_path
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Scan the module for Blueprint instances
        for name, obj in inspect.getmembers(module):
            if issubclass(type(obj), DecoratorApi):
                blueprints.append(obj)

    # If it's a single file, just process it
    if single_file:
        if not path.endswith(".py"):
            path += ".py"
        process_file(path)
    else:
        # Scan the path for .py files
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith(".py"):
                    file_path = os.path.join(root, file)
                    process_file(file_path)

            # If not recursive, break after the first path
            if not recursive:
                break

    return blueprints


app = FunctionApp(http_auth_level=AuthLevel.ANONYMOUS)
debug = os.environ.get("DEBUG", "false").lower() == "true"
site_name = os.environ.get("WEBSITE_SITE_NAME", os.environ.get("FUNCTION_NAME", ""))

for path in (
    BLUEPRINTS.get(site_name, [])
    + DEV_BLUEPRINTS
    + (BLUEPRINTS["debug"] if debug else [])
):
    for bp in find_blueprints(path):
        app.register_functions(bp)
