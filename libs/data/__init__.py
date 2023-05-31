from libs.utils.pluginloader import load
from typing import Any, List, Protocol, runtime_checkable
import inspect

# Define StorageProvider protocol. 
# Classes implementing this protocol must have save() and load() methods.
@runtime_checkable
class StorageProvider(Protocol):
    """
    Protocol for storage providers.

    Classes implementing this protocol must have save() and load() methods.
    """

    def save(self, *args, **kwargs) -> None:
        """
        Save data using the storage provider.

        Parameters
        ----------
        *args : tuple
            Additional positional arguments.
        **kwargs : dict
            Additional keyword arguments.
        """

        pass  # Placeholder for save method

    def load(self, *args, **kwargs) -> Any:
        """
        Load data using the storage provider.

        Parameters
        ----------
        *args : tuple
            Additional positional arguments.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        Any
            The loaded data.
        """

        pass  # Placeholder for load method


# Define StorageProviderRegistry protocol.
# Classes implementing this protocol must have the following class methods:
# get_protocol(), register(), get_instance(), get_schemes(), and load_modules().
@runtime_checkable
class StorageProviderRegistry(Protocol):
    """
    Protocol for storage provider registries.

    Classes implementing this protocol must have the following class methods:
    get_protocol(), register(), get_instance(), get_schemes(), and load_modules().
    """

    @classmethod
    def get_protocol(cls) -> Protocol:
        """
        Get the protocol associated with the registry.

        Returns
        -------
        Protocol
            The protocol associated with the registry.
        """

        pass  # Placeholder for get_protocol method

    @classmethod
    def register(cls, provider_class) -> None:
        """
        Register a storage provider class.

        Parameters
        ----------
        provider_class : class
            The storage provider class to register.
        """

        pass  # Placeholder for register method

    @classmethod
    def get_instance(cls, schema, *args, **kwargs) -> StorageProvider:
        """
        Get an instance of a storage provider supporting a specified schema.

        Parameters
        ----------
        schema : str
            The schema of the storage provider.
        *args : tuple
            Additional positional arguments.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        StorageProvider
            An instance of a storage provider supporting the specified schema.
        """

        pass  # Placeholder for get_instance method

    @classmethod
    def get_schemes(cls) -> List[str]:
        """
        Get a list of supported schemes.

        Returns
        -------
        List[str]
            A list of supported schemes.
        """

        pass  # Placeholder for get_schemes method

    @classmethod
    def load_modules(cls) -> None:
        """
        Load modules for the storage provider registry.
        """

        pass  # Placeholder for load_modules method


from libs.utils.pluginloader import load
import inspect

_REGISTRY = {}

for module in load(path=__file__, depth=1):
    for name, obj in inspect.getmembers(module):
        if isinstance(obj, StorageProviderRegistry):
            _REGISTRY[name] = obj
            obj.load_modules()  # Load modules for the storage provider registry

# Function to retrieve a provider instance supporting a specified scheme
def get_provider(protocol: str, scheme: str, *args, **kwargs) -> StorageProvider:
    """
    Get a provider instance supporting a specified scheme.

    Parameters
    ----------
    protocol : str
        The protocol associated with the provider instance.
    scheme : str
        The scheme supported by the provider instance.
    *args : tuple
        Additional positional arguments.
    **kwargs : dict
        Additional keyword arguments.

    Returns
    -------
    StorageProvider
        A provider instance supporting the specified scheme.
    """

    global _REGISTRY
    cls = _REGISTRY.get(protocol) or _REGISTRY.get(protocol + "Registry")
    if scheme in cls.get_schemes():
        return cls.get_instance(scheme, *args, **kwargs)
    if hasattr(cls, "regex_schemes"):
        if cls.regex_schemes(scheme):
            return cls.get_instance(scheme, *args, **kwargs)

# Function to retrieve a dictionary of supported schemes for each protocol
def get_supported() -> dict:
    """
    Get a dictionary of supported schemes for each protocol.

    Returns
    -------
    dict
        A dictionary mapping protocol names to lists of supported schemes.
    """

    global _REGISTRY
    return {cls.get_protocol().__name__: cls.get_schemes() for _, cls in _REGISTRY.items()}

# Initialize global bindings dictionary
_BINDINGS = {}

# Function to register a binding between a handle and a provider instance
def register_binding(handle: str, protocol: str, scheme: str, *args, **kwargs) -> None:
    """
    Register a binding between a handle and a provider instance.

    Parameters
    ----------
    handle : str
        The handle to bind.
    protocol : str
        The protocol associated with the provider instance.
    scheme : str
        The scheme supported by the provider instance.
    *args : tuple
        Additional positional arguments.
    **kwargs : dict
        Additional keyword arguments.
    """

    global _BINDINGS
    if handle not in _BINDINGS:
        _BINDINGS[handle] = get_provider(protocol, scheme, *args, **kwargs)

# Function to retrieve a provider instance from a binding using a handle
def from_bind(handle: str) -> StorageProvider:
    """
    Retrieve a provider instance from a binding using a handle.

    Parameters
    ----------
    handle : str
        The handle associated with the provider instance.

    Returns
    -------
    StorageProvider
        The provider instance associated with the handle, or None if not found.
    """

    global _BINDINGS
    if handle in _BINDINGS:
        return _BINDINGS[handle]
    return None
