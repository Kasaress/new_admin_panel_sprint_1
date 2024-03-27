import logging
import sqlite3
from contextlib import contextmanager


@contextmanager
def connection_context(db_path: str):
    connection = sqlite3.connect(db_path)
    yield connection
    connection.close()


logger = logging.getLogger('load_data')
logging.basicConfig(level=logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# File Handler
file_handler = logging.FileHandler('logs.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Console Handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
