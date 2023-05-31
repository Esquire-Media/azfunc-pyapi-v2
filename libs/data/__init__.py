from libs.utils.pluginloader import load
from typing import Any, List, Protocol, runtime_checkable
import inspect

# Define StorageProvider protocol. 
# Classes implementing this protocol must have save() and load() methods.
@runtime_checkable
class StorageProvider(Protocol):
    def save(self, *args, **kwargs) -> None:
        pass  # Placeholder for save method

    def load(self, *args, **kwargs) -> Any:
        pass  # Placeholder for load method

# Define StorageProviderRegistry protocol.
# Classes implementing this protocol must have the following class methods:
# get_protocol(), register(), get_instance(), get_schemes(), and load_modules().
@runtime_checkable
class StorageProviderRegistry(Protocol):
    @classmethod
    def get_protocol(cls) -> Protocol:
        pass  # Placeholder for get_protocol method

    @classmethod
    def register(cls, provider_class) -> None:
        pass  # Placeholder for register method

    @classmethod
    def get_instance(cls, schema, *args, **kwargs) -> StorageProvider:
        pass  # Placeholder for get_instance method

    @classmethod
    def get_schemes(cls) -> List[str]:
        pass  # Placeholder for get_schemes method

    @classmethod
    def load_modules(cls) -> None:
        pass  # Placeholder for load_modules method



# Initialize global registry dictionary
_REGISTRY = {}

# Load modules and register them if they implement the StorageProviderRegistry protocol
for module in load(path=__file__, depth=1):
    for name, obj in inspect.getmembers(module):
        if isinstance(obj, StorageProviderRegistry):
            _REGISTRY[name] = obj
            obj.load_modules()  # Load modules for the storage provider registry

# Function to retrieve a provider instance supporting a specified scheme
def get_provider(protocol: str, scheme: str, *args, **kwargs) -> StorageProvider:
    global _REGISTRY
    cls = _REGISTRY.get(protocol) or _REGISTRY.get(protocol + "Registry")
    if scheme in cls.get_schemes():
        return cls.get_instance(scheme, *args, **kwargs)
    if hasattr(cls, "regex_schemes"):
        if cls.regex_schemes(scheme):
            return cls.get_instance(scheme, *args, **kwargs)

# Function to retrieve a dictionary of supported schemes for each protocol
def get_supported() -> dict:
    global _REGISTRY
    return {cls.get_protocol().__name__: cls.get_schemes() for _, cls in _REGISTRY.items()}

# Initialize global bindings dictionary
_BINDINGS = {}

# Function to register a binding between a handle and a provider instance
def register_binding(handle: str, protocol: str, scheme: str, *args, **kwargs) -> None:
    global _BINDINGS
    if handle not in _BINDINGS:
        _BINDINGS[handle] = get_provider(protocol, scheme, *args, **kwargs)

# Function to retrieve a provider instance from a binding using a handle
def from_bind(handle: str) -> StorageProvider:
    global _BINDINGS
    if handle in _BINDINGS:
        return _BINDINGS[handle]
    return None
