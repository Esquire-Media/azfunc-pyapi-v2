from libs.utils.decorators import staticproperty
from libs.utils.threaded import current
from typing import Any, Callable, List


class ThreadKeyValueProvider:
    """
    Thread-based key-value storage provider.

    This class provides methods to save, load, and delete key-value pairs
    using thread-based storage mechanisms.
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

        return ["thread"]

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

    def __new__(cls, *args, **kwargs):
        """
        Create a new instance of ThreadKeyValueProvider.

        Parameters
        ----------
        *args : tuple
            Additional positional arguments.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        ThreadKeyValueProvider
            The created instance of ThreadKeyValueProvider.
        """

        if not hasattr(cls, "instance"):
            cls.instance = super(ThreadKeyValueProvider, cls).__new__(cls)
            return cls.instance

    def __getitem__(self, handle: str) -> Any:
        """
        Retrieve an item from the storage using a handle.

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
        Save a key-value pair in the storage.

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

        current.__setattr__(key, encoder(value) if encoder else value)

    def load(self, key: str, decoder: Callable = None, **kwargs) -> Any:
        """
        Load a value from the storage using a key.

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
            The loaded value.
        """

        return (
            decoder(current.__getattribute__(key)._get_current_object())
            if decoder
            else current.__getattribute__(key)._get_current_object()
        )

    def drop(self, key: str) -> None:
        """
        Delete a key-value pair from the storage.

        Parameters
        ----------
        key : str
            The key associated with the value to be deleted.
        """

        current.__delattr__(key)
