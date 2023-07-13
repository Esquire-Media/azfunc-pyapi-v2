from .http import HttpDecoratorApi
from azure.durable_functions.decorators.durable_app import Blueprint as DFBlueprint
from azure.functions import AuthLevel, FunctionRegister
from azure.functions.decorators.function_app import FunctionBuilder
from functools import wraps
from libs.utils.logging import AzureTableHandler
from typing import Callable, List
import importlib.util
import inspect
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


class Blueprint(DFBlueprint, HttpDecoratorApi):
    def logger(self, name: str = "azure.functions.decorator"):
        """
        Configure the logger for the decorator.

        Parameters
        ----------
        name : str, optional
            The name of the logger, by default "azure.functions.decorator".

        Returns
        -------
        Callable
            The decorator function.

        Notes
        -----
        This method configures the logger for the decorator.
        It sets up a logger with the specified name and adds an "info" method to log function execution details.

        Steps:
        1. Create a logger with the specified name.
        2. Define an "info" method within the decorator function.
        3. Inside the "info" method:
            - Retrieve the trigger argument from the function arguments.
            - Log the function execution details using the logger.
        4. Wrap the function builder with the decorator.
        5. Return the wrapped function builder.

        The wrapped function builder can be used to decorate HTTP trigger functions with logging functionality.
        """
        _logger = logging.getLogger(name)

        def info(fb: FunctionBuilder, *args, **kwargs):
            trigger_arg = kwargs.get(fb._function.get_trigger().name, None)
            _logger.info(
                "started",
                extra={
                    "context": {
                        "PartitionKey": fb._function.get_function_name(),
                        "RowKey": getattr(
                            kwargs.get("context", None), "invocation_id", None
                        ),
                        "Trigger": (trigger_type := fb._function.get_trigger().type),
                        "Payload": json.dumps(
                            {
                                k: getattr(trigger_arg, k)
                                for k in dir(trigger_arg)
                                if not k.startswith("_")
                            }
                            if trigger_type != "httpTrigger"
                            else {
                                "url": trigger_arg.url,
                                "method": trigger_arg.method,
                                "headers": dict(trigger_arg.headers),
                                "params": dict(trigger_arg.route_params),
                            }
                        ),
                    }
                },
            )

        @self._configure_function_builder
        def wrap(fb: FunctionBuilder):
            def decorator():
                # Wrap the function with the "info" method as the "before" middleware
                self._wrap_function(
                    fb, before=lambda *args, **kwargs: info(fb, *args, **kwargs)
                )
                return fb

            return decorator()

        return wrap

    def _wrap_function(
        self, fb: FunctionBuilder, before: Callable = None, after: Callable = None
    ):
        """
        Wrap the function with middleware functions.

        Parameters
        ----------
        fb : FunctionBuilder
            The function builder.
        before : Callable, optional
            The middleware function to be executed before the user-defined code, by default None.
        after : Callable, optional
            The middleware function to be executed after the user-defined code, by default None.

        Notes
        -----
        This method wraps the user-defined code with middleware functions.
        The middleware functions can be provided as arguments and will be executed before and after the user-defined code.

        Steps:
        1. Retrieve the user-defined code from the function builder.
        2. Define an async middleware function that wraps the user-defined code.
        3. Inside the middleware function:
            - If a "before" middleware function is provided:
                - Check if it is an asynchronous function or a regular function.
                - Execute the "before" middleware function with the user-defined code and arguments.
            - Execute the user-defined code (asynchronously or synchronously) and store the results.
            - If an "after" middleware function is provided:
                - Check if it is an asynchronous function or a regular function.
                - Execute the "after" middleware function with the results, user-defined code, and arguments.
                - Store the modified results.
            - Return the results.
        4. Assign the middleware function to the function builder.

        The user-defined code with middleware is assigned back to the function builder for execution.
        """
        user_code = fb._function._func

        # `wraps` This ensures we re-export the same method-signature as the decorated method
        @wraps(user_code)
        async def _middleware(*args, **kwargs):
            if before != None:
                if inspect.iscoroutinefunction(before):
                    await before(user_code, *args, **kwargs)
                else:
                    before(user_code, *args, **kwargs)
            if inspect.iscoroutinefunction(user_code):
                results = await user_code(*args, **kwargs)
            else:
                results = user_code(*args, **kwargs)
            if after != None:
                if inspect.iscoroutinefunction(after):
                    results = await after(results, *args, **kwargs)
                else:
                    results = after(results, *args, **kwargs)
            return results

        user_code_with_middleware = _middleware
        fb._function._func = user_code_with_middleware


class FunctionApp(Blueprint, FunctionRegister):
    def __init__(self, http_auth_level: AuthLevel | str = AuthLevel.FUNCTION):
        """
        Function app class for registering blueprints.

        Parameters
        ----------
        http_auth_level : AuthLevel | str, optional
            The authentication level for HTTP triggers, by default AuthLevel.FUNCTION.

        Notes
        -----
        This class provides a function app for registering blueprints.
        It extends the Blueprint and FunctionRegister classes to support HTTP triggers and function registration.

        Steps:
        1. Initialize the parent classes (Blueprint and FunctionRegister).
        2. Check if a configuration file named "config.py" exists.
        3. If the configuration file exists, load and execute it as a module.

        The FunctionApp class can be used to create a function app and register blueprints.
        """
        super().__init__(http_auth_level)
        if os.path.exists("config.py"):
            spec = importlib.util.spec_from_file_location("config", "config.py")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

    def register_blueprints(self, paths: List[str]):
        """
        Register blueprints from the specified paths.

        Parameters
        ----------
        paths : List[str]
            The paths to search for blueprints.

        Notes
        -----
        This method registers blueprints from the specified paths.
        It scans the paths for Python files and processes them as blueprints.

        Steps:
        1. Iterate over the paths.
        2. For each path:
            - Check if the path ends with "/*" or "/" to determine if it is recursive or a single file.
            - If it is a single file, process the file.
            - If it is recursive, scan the path for Python files and process each file.
        3. Return the list of registered blueprints.

        The register_blueprints method can be used to register multiple blueprints at once from different paths.
        """
        for p in paths:
            for bp in FunctionApp.find_blueprints(p):
                self.register_blueprint(bp)

    @staticmethod
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
