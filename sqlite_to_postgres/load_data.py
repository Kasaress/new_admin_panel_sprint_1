import sqlite3

import psycopg2
from psycopg2.extras import DictCursor

from config import CHUNK_SIZE, DSL, MAPPING_TABLE_DATACLASS, SQLITE_PATH
from exctactor import SQLiteExtractor
from helpers import logger
from loader import PostgresSaver
from sqlite_to_postgres.helpers import connection_context


def load_from_sqlite(connection: sqlite3.Connection, pg_conn):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)

    for table_name, data_class in MAPPING_TABLE_DATACLASS:
        logger.info(f'Старт копирования данных из таблицы {table_name}.')
        count = sqlite_extractor.get_count_rows(table_name)
        if not count:
            logger.warning(f'Таблица {table_name} не содержит данных.')
            continue
        logger.info(f'Таблица {table_name} содержит {count} строк.')
        chunk_size = CHUNK_SIZE
        start = 0
        while start < count:
            end = start + chunk_size
            if end > count:
                end = count
            logger.info(
                f'Старт выгрузки данных с {start} '
                f'по {end} строки из {count} строк из таблицы {table_name}.'
            )
            data = sqlite_extractor.extract_data(
                start,
                end,
                table_name,
                data_class
            )
            if not data:
                logger.warning(f'Не получены данные из таблицы {table_name}.')
                continue
            logger.info(
                f'Старт сохранения данных с {start} '
                f'по {end} строки из {count} строк из таблицы {table_name}.'
            )
            postgres_saver.save_all_data(data, table_name)
            start = end
        logger.info(f'Финиш копирования данных из таблицы {table_name}.')
    logger.info('Все данные из всех таблиц обработаны.')


if __name__ == '__main__':
    logger.info(f'Запуск скрипта с конфигом: {DSL}')
    with (
        connection_context(SQLITE_PATH) as sqlite_conn,
        psycopg2.connect(
            **DSL, cursor_factory=DictCursor
        ) as pg_conn  # type: ignore
    ):
        load_from_sqlite(sqlite_conn, pg_conn)
