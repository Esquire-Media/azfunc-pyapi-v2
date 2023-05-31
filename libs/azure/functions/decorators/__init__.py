from .http import HttpDecoratorApi
from azure.durable_functions import BluePrint as DFBP
from azure.functions import AuthLevel, FunctionRegister
from azure.functions.decorators.function_app import FunctionBuilder
from functools import wraps
from libs.utils.logging import AzureTableHandler
from typing import Callable, List
from azure.functions.decorators.function_app import FunctionBuilder
from functools import wraps
from libs.utils.logging import AzureTableHandler
from typing import Callable, List
import importlib.util
import inspect
import logging
import logging
import os
try:
    import simplejson as json
except:
    import json


__handler = AzureTableHandler()
__logger = logging.getLogger("azure.functions.decorator")
if __handler not in __logger.handlers:
    __logger.addHandler(__handler)


class Blueprint(DFBP, HttpDecoratorApi):
    pass

        user_code_with_middleware = _middleware
        fb._function._func = user_code_with_middleware

class FunctionApp(Blueprint, FunctionRegister):
    def __init__(self, http_auth_level: AuthLevel | str = AuthLevel.FUNCTION):
        super().__init__(http_auth_level)
        if os.path.exists("config.py"):
            spec = importlib.util.spec_from_file_location("config", "config.py")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

    def register_blueprints(self, paths: List[str]):
        for p in paths:
            for bp in FunctionApp.find_blueprints(p):
                self.register_blueprint(bp)

    @staticmethod
    def find_blueprints(path):
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
            single_file = (
                True  # If it doesn't end with / or /*, assume it's a single file
            )

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
                if isinstance(obj, Blueprint):
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
