import functools
from libs.utils.decorators import staticproperty, immutable_arguments
from libs.utils.pluginloader import load
from typing import Any, List, Protocol, runtime_checkable
import inspect


@runtime_checkable
class StructuredProvider(Protocol):
    """
    Protocol for structured data storage provider.

    Classes implementing this protocol must define methods for storing, loading, and dropping structured data.
    """

    @staticproperty
    def SUPPORTED_SCHEMES(self) -> list:
        """
        List of supported schemes.

        Returns
        -------
        list
            A list of supported schemes.
        """

        pass

    @staticproperty
    def RESOURCE_TYPE_DELIMITER(self) -> list:
        """
        List of resource type delimiters.

        Returns
        -------
        list
            A list of resource type delimiters.
        """

        pass

    def get_schema(self, type_: str) -> dict:
        """
        Get the schema for a given structure type.

        Parameters
        ----------
        type_ : str
            The type of the structure.

        Returns
        -------
        dict
            A dictionary representing the structure's schema.
        """

        pass

    def save(self, key: str, value: Any, **kwargs) -> None:
        """
        Store the given value with the specified key in the storage provider.

        Parameters
        ----------
        key : str
            The key associated with the value.
        value : Any
            The value to be stored.
        **kwargs : dict
            Additional keyword arguments.
        """

        pass

    def load(self, key: str, **kwargs) -> Any:
        """
        Retrieve the value associated with the specified key from the storage provider.

        Parameters
        ----------
        key : str
            The key associated with the value.

        Returns
        -------
        Any
            The retrieved value.
        """

        pass

    def drop(self, key: str, **kwargs) -> None:
        """
        Delete the value associated with the specified key from the storage provider.

        Parameters
        ----------
        key : str
            The key associated with the value.
        **kwargs : dict
            Additional keyword arguments.
        """

        pass


@runtime_checkable
class StructuredQueryFrame(Protocol):
    """
    Protocol for structured query frames.

    Classes implementing this protocol must define methods for indexing, calling, obtaining length, and converting to pandas.
    """

    def __getitem__(self):
        """
        Get an item using indexing.

        Returns
        -------
        Any
            The retrieved item.
        """

        pass

    def __call__(self, key: str = None):
        """
        Call the query frame.

        Parameters
        ----------
        key : str, optional
            The key associated with the value, by default None.
        """

        pass

    def __len__(self) -> int:
        """
        Get the length of the query frame.

        Returns
        -------
        int
            The length of the query frame.
        """

        pass

    def to_pandas(self):
        """
        Convert the query frame to a pandas DataFrame.

        Returns
        -------
        pandas.DataFrame
            The converted pandas DataFrame.
        """

        pass


class StructuredRegistry:
    """
    Registry for structured data storage providers.

    This class provides methods for registering, getting instances, and loading modules for structured data storage providers.
    """

    _providers = []

    @classmethod
    def get_protocol(cls) -> Protocol:
        """
        Get the protocol for structured data storage providers.

        Returns
        -------
        Protocol
            The protocol for structured data storage providers.
        """

        return StructuredProvider

    @classmethod
    def register(cls, provider_class) -> None:
        """
        Register a structured data storage provider class.

        Parameters
        ----------
        provider_class : class
            The provider class to register.
        """

        if not inspect.isclass(provider_class) or not isinstance(
            provider_class, StructuredProvider
        ):
            raise TypeError("Only Structured Providers can be registered.")
        if provider_class not in cls._providers:
            cls._providers.append(provider_class)

    @classmethod
    @immutable_arguments
    @functools.cache
    def get_instance(cls, scheme: str, *args, **kwargs) -> StructuredProvider:
        """
        Get an instance of a structured data storage provider.

        Parameters
        ----------
        scheme : str
            The scheme associated with the provider.
        *args : tuple
            Additional positional arguments.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        StructuredProvider
            An instance of the structured data storage provider.

        Example
        -------
        >>> from libs.data import from_bind
        >>> provider = from_bind('structured_handle')

        Notes
        -----
        This method generally should not be called directly.

        This method retrieves an instance of a structured data storage provider based on the specified scheme.
        The scheme is used to identify the appropriate provider class.
        Additional arguments and keyword arguments are passed to the provider class constructor.
        """

        provider_class = None
        for provider in cls._providers:
            if scheme in provider.SUPPORTED_SCHEMES:
                provider_class = provider
                break
            if cls.regex_schemes(scheme):
                provider_class = provider
                break
        # if not provider_class:
        #     raise ValueError(
        #         f"Storage provider for the '{scheme}' scheme is not supported."
        #     )
        return provider_class(scheme=scheme, *args, **kwargs)

    @classmethod
    def get_schemes(cls) -> List[str]:
        """
        Get a list of supported schemes.

        Returns
        -------
        List[str]
            A list of supported schemes.

        Example
        -------
        >>> schemes = StructuredRegistry.get_schemes()
        >>> print(schemes)

        Notes
        -----
        This method returns a list of all supported schemes by querying each registered provider.
        """

        return [
            scheme
            for provider in cls._providers
            for scheme in provider.SUPPORTED_SCHEMES
        ]

    @classmethod
    def regex_schemes(cls, scheme: str) -> bool:
        """
        Check if a scheme matches any supported regular expression schemes.

        Parameters
        ----------
        scheme : str
            The scheme to check.

        Returns
        -------
        bool
            True if the scheme matches a regular expression scheme, False otherwise.

        Example
        -------
        >>> match = StructuredRegistry.regex_schemes("my_scheme")
        >>> print(match)

        Notes
        -----
        This method checks if the specified scheme matches any of the regular expression schemes defined by the registered providers.
        """

        for provider in cls._providers:
            if hasattr(provider, "SUPPORTED_SCHEMES_REGEX"):
                if provider.SUPPORTED_SCHEMES_REGEX(scheme):
                    return True
        return False

    @classmethod
    def load_modules(cls) -> None:
        """
        Load modules and register any Structured Providers found.

        Notes
        -----
        This method loads modules using the pluginloader utility and registers any structured data storage providers found in the modules.
        """

        for module in load(path=__file__, file_mode="all", depth=-1):
            for _, obj in inspect.getmembers(module):
                if (
                    inspect.isclass(obj)
                    and isinstance(obj, StructuredProvider)
                    and obj != StructuredProvider
                ):
                    StructuredRegistry.register(obj)
