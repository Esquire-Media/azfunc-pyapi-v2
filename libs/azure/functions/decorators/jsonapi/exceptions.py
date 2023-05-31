class JsonApiMethodException(Exception):
    def __init__(self, *args: object) -> None:
        """
        Exception class for JSON API method not allowed.

        Parameters
        ----------
        *args
            Positional arguments passed to the base class constructor.

        Notes
        -----
        This exception is raised when an HTTP method is not supported by the JSON API specification.
        The status code is set to 405 (Method Not Allowed) and provides an error message indicating the supported methods.

        References
        ----------
        - JSONAPI Specification: https://jsonapi.org/format/#crud
        """
        super().__init__(*args)
        self.status_code = 405
        self.errors = [
            {
                "status": "405",
                "source": {"pointer": "http/method"},
                "title": "Method Not Allowed",
                "detail": "Method not allowed. Supported methods are: GET, POST, PATCH, DELETE",
            }
        ]


class JsonApiHeaderContentTypeException(Exception):
    def __init__(self, *args: object) -> None:
        """
        Exception class for JSON API unsupported media type (Content-Type) error.

        Parameters
        ----------
        *args
            Positional arguments passed to the base class constructor.

        Notes
        -----
        This exception is raised when the Content-Type header is not set to "application/vnd.api+json"
        as required by the JSON API specification.
        The status code is set to 415 (Unsupported Media Type) and provides an error message indicating the correct Content-Type.

        References
        ----------
        - JSONAPI Specification: https://jsonapi.org/format/#content-negotiation
        """
        super().__init__(*args)
        self.status_code = 415
        self.errors = [
            {
                "status": "415",
                "source": {"pointer": "http/header/content-type"},
                "title": "Unsupported Media Type",
                "detail": "Content-Type must be set to application/vnd.api+json",
            }
        ]


class JsonApiHeaderAcceptException(Exception):
    def __init__(self, *args: object) -> None:
        """
        Exception class for JSON API not acceptable (Accept header) error.

        Parameters
        ----------
        *args
            Positional arguments passed to the base class constructor.

        Notes
        -----
        This exception is raised when the Accept header is not set to "application/vnd.api+json"
        as required by the JSON API specification.
        The status code is set to 406 (Not Acceptable) and provides an error message indicating the correct Accept header.

        References
        ----------
        - JSONAPI Specification: https://jsonapi.org/format/#content-negotiation
        """
        super().__init__(*args)
        self.status_code = 406
        self.errors = [
            {
                "status": "406",
                "source": {"pointer": "http/header/accept"},
                "title": "Not Acceptable",
                "detail": "Accept header must be set to application/vnd.api+json",
            }
        ]


class JsonApiInvalidPayloadException(Exception):
    def __init__(self, *args: object) -> None:
        """
        Exception class for JSON API invalid JSON payload error.

        Parameters
        ----------
        *args
            Positional arguments passed to the base class constructor.

        Notes
        -----
        This exception is raised when the body of the request is not a valid JSON.
        The status code is set to 400 (Bad Request) and provides an error message indicating the invalid JSON payload.

        References
        ----------
        - JSONAPI Specification: https://jsonapi.org/format/#document-structure
        """
        super().__init__(*args)
        self.status_code = 400
        self.errors = [
            {
                "status": "400",
                "source": {"pointer": "http/body"},
                "title": "Invalid JSON Payload",
                "detail": "The body of the request is not valid JSON",
            }
        ]


class JsonApiInvalidVersionException(Exception):
    def __init__(self, *args: object) -> None:
        """
        Exception class for JSON API invalid JSONAPI version error.

        Parameters
        ----------
        *args
            Positional arguments passed to the base class constructor.

        Notes
        -----
        This exception is raised when the JSONAPI version specified in the request does not match the expected version.
        The status code is set to 400 (Bad Request) and provides an error message indicating the expected JSONAPI version.

        References
        ----------
        - JSONAPI Specification: https://jsonapi.org/format/#versioning
        """
        super().__init__(*args)
        self.status_code = 400
        self.errors = [
            {
                "status": "400",
                "source": {"pointer": "http/body"},
                "title": "Invalid JSONAPI Version",
                "detail": f"JSONAPI version must be set to {args[0]}",
            }
        ]


class JsonApiInvalidEmptyPayloadException(Exception):
    def __init__(self, *args: object) -> None:
        """
        Exception class for JSON API invalid empty payload error.

        Parameters
        ----------
        *args
            Positional arguments passed to the base class constructor.

        Notes
        -----
        This exception is raised when the body of the request should contain an empty JSON object,
        but it contains other data instead.
        The status code is set to 400 (Bad Request) and provides an error message indicating the invalid payload.

        References
        ----------
        - JSONAPI Specification: https://jsonapi.org/format/#crud-creating-empty-resources
        """
        super().__init__(*args)
        self.status_code = 400
        self.errors = [
            {
                "status": "400",
                "source": {"pointer": "http/body"},
                "title": "Invalid Payload",
                "detail": "Body should contain an empty JSON object",
            }
        ]


class JsonApiInvalidRequestException(Exception):
    def __init__(self, *args: object) -> None:
        """
        Exception class for JSON API invalid request error.

        Parameters
        ----------
        *args
            Positional arguments passed to the base class constructor.

        Notes
        -----
        This exception is raised when the JSON API request is missing required 'data', 'type', or 'id' values.
        The status code is set to 400 (Bad Request) and provides an error message indicating the missing values.

        References
        ----------
        - JSONAPI Specification: https://jsonapi.org/format/#crud-creating
        """
        super().__init__(*args)
        self.status_code = 400
        self.errors = [
            {
                "status": "400",
                "source": {"pointer": "http/body"},
                "title": "Invalid Request",
                "detail": "Request is missing 'data', 'type', or 'id' values",
            }
        ]
