from typing import Any, List
import azure.functions as func


class HttpRequest(func.HttpRequest):
    def __init__(self, *args, **kwargs) -> None:
        """
        Initializes a custom HttpRequest object.

        Parameters
        ----------
        *args
            Positional arguments passed to the base class constructor.
        **kwargs
            Keyword arguments passed to the base class constructor.
            identity : dict, optional
                The identity information associated with the request.
            jsonapi : dict, optional
                Additional JSON API related information.
            session : dict, optional
                Session information associated with the request.
        """
        self.identity: dict = (kwargs.pop("identity", None),)
        self.jsonapi: dict = (kwargs.pop("jsonapi", None),)
        self.session: dict = (kwargs.pop("session", None),)
        super().__init__(*args, **kwargs)


class HttpResponse(func.HttpResponse):
    def __init__(self, *args, **kwargs) -> None:
        """
        Initializes a custom HttpResponse object.

        Parameters
        ----------
        *args
            Positional arguments passed to the base class constructor.
        **kwargs
            Keyword arguments passed to the base class constructor.
            resources : List[Any], optional
                List of additional resources associated with the response.
        """
        self.resources: List[Any] = (kwargs.pop("resources", None),)
        super().__init__(*args, **kwargs)

    def set_body(self, *args, **kwargs):
        """
        Sets the body of the HttpResponse.

        Parameters
        ----------
        *args
            Positional arguments passed to the internal __set_body method.
        **kwargs
            Keyword arguments passed to the internal __set_body method.
        """
        self.__set_body(*args, **kwargs)
