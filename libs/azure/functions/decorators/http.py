from ..http import HttpResponse
from .jsonapi import (
    parse_request as parse_request_jsonapi,
    alter_response as alter_response_jsonapi,
)
from .session import (
    parse_request as parse_request_session,
    alter_response as alter_response_session,
)
from asyncio import iscoroutinefunction
from azure.functions.decorators.core import BindingDirection
from azure.functions.decorators.constants import HTTP_TRIGGER, HTTP_OUTPUT
from azure.functions.decorators.function_app import DecoratorApi
from functools import wraps
from typing import Any, Callable, Optional
import os


class HttpDecoratorApi(DecoratorApi):
    @staticmethod
    def get_request_binding(wrap):
        """
        Get the request binding from the function wrapper.

        Parameters
        ----------
        wrap : Callable
            The function wrapper.

        Returns
        -------
        Any
            The request binding if found, None otherwise.

        Notes
        -----
        This method retrieves the request binding from the function wrapper's bindings.
        It checks the binding direction and type to determine if it is an incoming HTTP trigger binding.
        """
        for binding in wrap._function.get_bindings():
            if (
                binding._direction == BindingDirection.IN
                and binding.type == HTTP_TRIGGER
            ):
                return binding
        return None

    @staticmethod
    def get_response_binding(wrap):
        """
        Get the response binding from the function wrapper.

        Parameters
        ----------
        wrap : Callable
            The function wrapper.

        Returns
        -------
        Any
            The response binding if found, None otherwise.

        Notes
        -----
        This method retrieves the response binding from the function wrapper's bindings.
        It checks the binding direction and type to determine if it is an outgoing HTTP output binding.
        """
        for binding in wrap._function.get_bindings():
            if (
                binding._direction == BindingDirection.OUT
                and binding.type == HTTP_OUTPUT
            ):
                return binding
        return None

    def _enhance_http_request(
        self,
        wrap: Callable,
        property_name: str,
        value: Any = None,
        func: Callable = None,
        **func_kwargs,
    ) -> None:
        """
        Enhance the HTTP request object with additional properties or functions.

        Parameters
        ----------
        wrap : Callable
            The function wrapper.
        property_name : str
            The name of the property to be added to the request object.
        value : Any, optional
            The value of the property, by default None.
        func : Callable, optional
            The function to be called to compute the value of the property, by default None.
        **func_kwargs
            Additional keyword arguments to be passed to the function.

        Notes
        -----
        This method enhances the HTTP request object by adding a new property or function to it.
        The property name and value can be specified directly, or a function can be provided to compute the value dynamically.
        The enhanced HTTP request object is then passed to the user's code.

        Steps:
        1. Check if there is an HTTP trigger binding in the function wrapper.
        2. Get the user-defined code from the function wrapper.
        3. Check if the user-defined code is a coroutine function (async) or a regular function.
        4. Define a middleware function that wraps the user-defined code.
            - Retrieve the HTTP request object from the function arguments.
            - Set the specified property name on the request object.
                - If a value is provided directly, use it.
                - If a function is provided, call it with the request object and additional function arguments.
        5. Call the user-defined code (either asynchronously or synchronously) with the modified arguments.
        6. Return the result of the user-defined code.

        The enhanced user-defined code (middleware) is assigned back to the function wrapper.
        """
        if binding := self.get_request_binding(wrap):
            user_code = wrap._function._func

            if iscoroutinefunction(user_code):
                # Asynchronous middleware function
                @wraps(user_code)
                async def middleware(*args, **kwargs):
                    # Set the specified property on the request object
                    setattr(
                        kwargs[binding.name],
                        property_name,
                        value or func(kwargs[binding.name], **func_kwargs),
                    )
                    # Call the user-defined code
                    return await user_code(*args, **kwargs)

            else:
                # Synchronous middleware function
                @wraps(user_code)
                def middleware(*args, **kwargs):
                    # Set the specified property on the request object
                    setattr(
                        kwargs[binding.name],
                        property_name,
                        value or func(kwargs[binding.name], **func_kwargs),
                    )
                    # Call the user-defined code
                    return user_code(*args, **kwargs)

            # Assign the enhanced user-defined code (middleware) back to the function wrapper
            enhanced_user_code = middleware
            wrap._function._func = enhanced_user_code

    def _enhance_http_response(
        self,
        wrap: Callable,
        property_name: str = None,
        value: Any = None,
        func: Callable = None,
        **func_kwargs,
    ):
        """
        Enhance the HTTP response object with additional properties or functions.

        Parameters
        ----------
        wrap : Callable
            The function wrapper.
        property_name : str, optional
            The name of the property to be added to the response object, by default None.
        value : Any, optional
            The value of the property, by default None.
        func : Callable, optional
            The function to be called to compute the value of the property, by default None.
        **func_kwargs
            Additional keyword arguments to be passed to the function.

        Notes
        -----
        This method enhances the HTTP response object by adding a new property or function to it.
        The property name and value can be specified directly, or a function can be provided to compute the value dynamically.
        The enhanced HTTP response object is then returned to the user's code.

        Steps:
        1. Check if there is an HTTP output binding in the function wrapper.
        2. Get the user-defined code from the function wrapper.
        3. Check if the user-defined code is a coroutine function (async) or a regular function.
        4. Define a middleware function that wraps the user-defined code.
            - Retrieve the HTTP request object from the function arguments.
            - Set the specified property on the response object.
                - If a value is provided directly, use it.
                - If a function is provided, call it with the response object and additional function arguments.
        5. Call the user-defined code (either asynchronously or synchronously) with the modified arguments.
        6. If the response binding name is "$return":
            - Modify the response object based on the specified property and value or function.
            - Return the modified response object.
        7. Return the result of the user-defined code.

        The enhanced user-defined code (middleware) is assigned back to the function wrapper.
        """
        if binding := self.get_response_binding(wrap):
            user_code = wrap._function._func

            if iscoroutinefunction(user_code):
                # Asynchronous middleware function
                @wraps(user_code)
                async def middleware(*args, **kwargs):
                    # Add the request object to the function arguments
                    func_kwargs["request"] = kwargs[self.get_request_binding(wrap).name]
                    if binding.name == "$return":
                        # Call the user-defined code
                        response: HttpResponse = await user_code(*args, **kwargs)
                        # Modify the response object based on the specified property and value or function
                        if property_name:
                            setattr(
                                response,
                                property_name,
                                value or func(response, **func_kwargs),
                            )
                        else:
                            response: HttpResponse = value or func(
                                response, **func_kwargs
                            )
                        # Return the modified response object
                        return response
                    else:
                        # Call the user-defined code without modifying the response object
                        return await user_code(*args, **kwargs)

            else:
                # Synchronous middleware function
                @wraps(user_code)
                def middleware(*args, **kwargs):
                    # Add the request object to the function arguments
                    func_kwargs["request"] = kwargs[self.get_request_binding(wrap).name]
                    if binding.name == "$return":
                        # Call the user-defined code
                        results = user_code(*args, **kwargs)
                        # Modify the response object based on the specified property and value or function
                        if property_name:
                            setattr(
                                results,
                                property_name,
                                value or func(results, **func_kwargs),
                            )
                        else:
                            results = value or func(results, **func_kwargs)
                        # Return the modified response object
                        return results
                    else:
                        # Call the user-defined code without modifying the response object
                        return user_code(*args, **kwargs)

            # Assign the enhanced user-defined code (middleware) back to the function wrapper
            enhanced_user_code = middleware
            wrap._function._func = enhanced_user_code

    def jsonapi(self) -> Callable:
        """
        Decorator to enhance HTTP triggers with JSON API support.

        Returns
        -------
        Callable
            The decorator function.

        Notes
        -----
        This decorator enhances HTTP triggers with JSON API support.
        It adds the necessary route parameters for JSON API routing and enhances the request and response objects
        with JSON API parsing and alteration functions.

        Steps:
        1. Configure the function builder for the decorator.
        2. Define the decorator function.
            - Append the JSON API route parameters to the existing route.
            - Enhance the HTTP request object with the `jsonapi` property using the `parse_request_jsonapi` function.
            - Enhance the HTTP response object using the `alter_response_jsonapi` function.
        3. Return the decorated function.

        The decorator function is returned to be used as a decorator for HTTP trigger functions.

        References
        ----------
        - JSON API Specification: https://jsonapi.org/
        """

        @self._configure_function_builder
        def wrap(fb):
            def decorator():
                # Append the JSON API route parameters to the existing route
                self.get_request_binding(
                    fb
                ).route += (
                    "/{resource_type}/{resource_id?}/{relation_type?}/{relation_id?}"
                )
                # Enhance the HTTP request object with the `jsonapi` property using `parse_request_jsonapi`
                self._enhance_http_request(
                    wrap=fb, property_name="jsonapi", func=parse_request_jsonapi
                )
                # Enhance the HTTP response object using `alter_response_jsonapi`
                self._enhance_http_response(wrap=fb, func=alter_response_jsonapi)
                return fb

            return decorator()

        return wrap

    def session(
        self,
        secret: Optional[str] = os.environ.get("SESSION_SECRET"),
        max_age: Optional[int] = int(os.environ.get("SESSION_MAX_AGE")) or 3600,
        **kwargs,
    ):
        """
        Decorator to enhance HTTP triggers with session support.

        Parameters
        ----------
        secret : Optional[str], optional
            The session secret key, by default None.
        max_age : Optional[int], optional
            The session maximum age in seconds, by default None.
        **kwargs
            Additional keyword arguments.

        Returns
        -------
        Callable
            The decorator function.

        Notes
        -----
        This decorator enhances HTTP triggers with session support.
        It enhances the request and response objects with session parsing and alteration functions.
        The session secret and maximum age can be provided as arguments or retrieved from environment variables.

        Steps:
        1. Configure the function builder for the decorator.
        2. Define the decorator function.
        3. Inside the decorator function:
            - Enhance the HTTP request object with the `session` property using the `parse_request_session` function.
                - Pass the session `secret`, `max_age`, and additional keyword arguments.
            - Enhance the HTTP response object using the `alter_response_session` function.
                - Pass the session `secret`, `max_age`, and additional keyword arguments.
        4. Return the decorated function.

        The decorator function is returned to be used as a decorator for HTTP trigger functions.
        """

        @self._configure_function_builder
        def wrap(fb):
            def decorator():
                # Enhance the HTTP request object with the `session` property using `parse_request_session`
                self._enhance_http_request(
                    wrap=fb,
                    property_name="session",
                    func=parse_request_session,
                    secret=secret,
                    max_age=max_age,
                    **kwargs,
                )
                # Enhance the HTTP response object using `alter_response_session`
                self._enhance_http_response(
                    wrap=fb,
                    func=alter_response_session,
                    secret=secret,
                    max_age=max_age,
                )
                return fb

            return decorator()

        return wrap
