from pathlib import Path
import logging
import inspect

class LocalFileHandler(logging.Handler):        
    def emit(self, record):
        # Get the name of the module where the logging call was made
        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        path = str(Path(module.__file__).relative_to(Path.cwd()))

        # Get or create the logger
        logger = self.get_or_create(path)

        # Format the record and write it to the file
        msg = self.format(record)
        logger.handle(msg)
    
    @staticmethod
    def get_or_create(path: str):
        logger = logging.getLogger(path)
        if not logger.handlers:
            path = Path(f"logs/{path}.txt")
            path.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(str(path))
            fh.setLevel(logging.DEBUG)
            logger.addHandler(fh)
        return logger
