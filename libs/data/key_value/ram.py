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
        """

        return decoder(self.store[key], **kwargs) if decoder else self.store[key]

    def drop(self, key: str) -> None:
        """
        Delete a key-value pair from the store.

        Parameters
        ----------
        key : str
            The key associated with the value to be deleted.
        """

        del self.store[key]
