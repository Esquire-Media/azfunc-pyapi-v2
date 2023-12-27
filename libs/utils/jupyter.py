# File: libs/utils/jupyter.py

from typing import Optional
import asyncio


def is_running_in_jupyter_notebook():
    try:
        # Check if IPython is available
        from IPython import get_ipython

        ipython = get_ipython()

        # IPython is available, now check if it's a Jupyter kernel
        if "IPKernelApp" in ipython.config:  # IPython version >= 7.0
            return True
        if hasattr(ipython, "kernel"):  # IPython version < 7.0
            return True
    except (NameError, ImportError, AttributeError) as e:
        pass
    return False


def get_jupyter_event_loop() -> Optional[asyncio.AbstractEventLoop]:
    try:
        
        # Check if running in Jupyter notebook
        if not is_running_in_jupyter_notebook():
            return None  # Not running in Jupyter notebook

        # Get the existing loop
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                # If the existing loop is closed, create a new one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
        except RuntimeError:
            # If no current event loop, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop
    except Exception as e:
        # Handle any exceptions
        print(f"Error occurred: {e}")
        return None
