from libs.utils.decorators import staticproperty
from typing import Any, Callable, List


class MemoryKeyValueProvider:
    """
    Memory-based key-value storage provider.

    This class provides methods to save, load, and delete key-value pairs
    in a memory-based store.
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

        return ["ram"]

    @staticproperty
    def scheme(self) -> str:
        """
        Scheme supported by the provider.

        Returns
        -------
        str
            The supported scheme.
        """

        return self.SUPPORTED_SCHEMES[0]

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize an instance of MemoryKeyValueProvider.

        Parameters
        ----------
        *args : tuple
            Additional positional arguments.
        **kwargs : dict
            Additional keyword arguments.
        """

        self.store = {}

    def __getitem__(self, handle: str) -> Any:
        """
        Retrieve an item from the store using a handle.

        Parameters
        ----------
        handle : str
            The handle associated with the item.

        Returns
        -------
        Any
            The retrieved item.

        Example
        -------
        >>> from libs.data import from_bind
        >>> provider = from_bind('ram_handle')
        >>> provider.save("my_key", "my_value")
        >>> value = provider["my_key"]
        >>> print(value)

        Notes
        -----
        This method allows retrieving an item from the store using the handle as a key.
        It is a shorthand for calling the `load()` method with the provided handle.
        """

        return self.load(key=handle)

    def save(self, key: str, value: Any, encoder: Callable = None, **kwargs) -> None:
        """
        Save a key-value pair in the store.

        Parameters
        ----------
        key : str
            The key associated with the value.
        value : Any
            The value to be saved.
        encoder : Callable, optional
            The encoder function to use for encoding the value, by default None.
        **kwargs : dict
            Additional keyword arguments.

        Example
        -------
        >>> from libs.data import from_bind
        >>> provider = from_bind('ram_handle')
        >>> provider.save("my_key", "my_value")

        Notes
        -----
        This method saves a key-value pair in the store. If an encoder function is provided,
        the value is encoded before saving. The encoded value or the original value is stored
        in the internal store dictionary with the specified key.
        """

        self.store[key] = encoder(value, **kwargs) if encoder else value

    def load(self, key: str, decoder: Callable = None, **kwargs) -> Any:
        """
        Load a value from the store using a key.

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

        Example
        -------
        >>> from libs.data import from_bind
        >>> provider = from_bind('ram_handle')
        >>> provider.save("my_key", "my_value")
        >>> value = provider.load("my_key")
        >>> print(value)

        Notes
        -----
        This method retrieves a value from the store using the specified key.
        If a decoder function is provided, the retrieved value is decoded before returning.
        The decoded value or the original stored value is returned.
        """

        return decoder(self.store[key], **kwargs) if decoder else self.store[key]

    def drop(self, key: str) -> None:
        """
        Delete a key-value pair from the store.

        Parameters
        ----------
        key : str
            The key associated with the value to be deleted.

        Example
        -------
        >>> from libs.data import from_bind
        >>> provider = from_bind('ram_handle')
        >>> provider.save("my_key", "my_value")
        >>> provider.drop("my_key")

        Notes
        -----
        This method deletes a key-value pair from the store using the specified key.
        If the key is not found in the store, a KeyError is raised.
        """

        del self.store[key]
