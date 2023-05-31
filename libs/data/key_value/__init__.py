from libs.utils.decorators import staticproperty, immutable_arguments
from typing import Any, Callable, List, Protocol, runtime_checkable
from libs.utils.pluginloader import load
import functools
import inspect

_REGISTRY = {}


@runtime_checkable
class KeyValueProvider(Protocol):
    """
    Protocol for key-value storage providers.

    Classes implementing this protocol must have the save() and load() methods.
    """

    @staticproperty
    def SUPPORTED_SCHEMES(self) -> List[str]:
        """
        List of supported schemes.

        Returns
        -------
        List[str]
            A list of supported schemes.
        """

        pass

    def save(self, key: str, value: Any, encoder: Callable = None, **kwargs) -> None:
        """
        Store the given value with the specified key in the storage provider.

        Parameters
        ----------
        key : str
            The key associated with the value.
        value : Any
            The value to be stored.
        encoder : Callable, optional
            The encoder function to use for encoding the value, by default None.
        **kwargs : dict
            Additional keyword arguments.
        """

        pass

    def load(self, key: str, decoder: Callable = None, **kwargs) -> Any:
        """
        Retrieve the value associated with the specified key from the storage provider.

        Parameters
        ----------
        key : str
            The key associated with the value.
        decoder : Callable, optional
            The decoder function to use for decoding the value, by default None.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        Any
            The retrieved value.
        """

        pass


class KeyValueRegistry:
    """
    Registry for key-value storage providers.

    This class provides methods for registering providers, getting instances,
    and retrieving supported schemes.
    """

    _providers = []

    @classmethod
    def get_protocol(cls) -> Protocol:
        """
        Get the protocol associated with the registry.

        Returns
        -------
        Protocol
            The protocol associated with the registry.
        """

        return KeyValueProvider

    @classmethod
    def register(cls, provider_class) -> None:
        """
        Register a key-value storage provider class.

        Parameters
        ----------
        provider_class : class
            The key-value storage provider class to register.

        Raises
        ------
        TypeError
            If the provider class is not a subclass of KeyValueProvider.
        """

        if not inspect.isclass(provider_class) or not isinstance(
            provider_class, KeyValueProvider
        ):
            raise TypeError("Only KeyValueProviders can be registered.")
        if provider_class not in cls._providers:
            cls._providers.append(provider_class)

    @classmethod
    @immutable_arguments
    @functools.cache
    def get_instance(cls, scheme, *args, **kwargs) -> KeyValueProvider:
        """
        Get an instance of a key-value storage provider supporting a specified scheme.

        Parameters
        ----------
        scheme : str
            The scheme supported by the provider.
        *args : tuple
            Additional positional arguments.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        KeyValueProvider
            An instance of a key-value storage provider supporting the specified scheme.

        Raises
        ------
        ValueError
            If no supporting provider is found for the specified scheme.
        """

        provider_class = None
        for provider in cls._providers:
            if scheme in provider.SUPPORTED_SCHEMES:
                provider_class = provider
                break
        if not provider_class:
            raise ValueError(
                f"Storage provider for the '{scheme}' scheme is not supported."
            )
        return provider_class(scheme=scheme, *args, **kwargs)

    @classmethod
    def get_schemes(cls) -> List[str]:
        """
        Get a list of all supported schemes.

        Returns
        -------
        List[str]
            A list of supported schemes.
        """

        return [
            scheme
            for provider in cls._providers
            for scheme in provider.SUPPORTED_SCHEMES
        ]

    @classmethod
    def load_modules(cls) -> None:
        """
        Load modules and register any KeyValueProviders found.
        """

        for module in load(path=__file__, file_mode="all", depth=-1):
            for name, obj in inspect.getmembers(module):
                if (
                    inspect.isclass(obj)
                    and isinstance(obj, KeyValueProvider)
                    and obj != KeyValueProvider
                ):
                    KeyValueRegistry.register(obj)
