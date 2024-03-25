import sqlite3

import psycopg2
from psycopg2.extras import DictCursor

from constants import CHUNK_SIZE, MAPPING_TABLE_DATACLASS
from exctactor import SQLiteExtractor
from loader import PostgresSaver


def load_from_sqlite(connection: sqlite3.Connection, pg_conn):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)

    for table_name, data_class in MAPPING_TABLE_DATACLASS:
        print(table_name, data_class)
        count = sqlite_extractor.get_count_rows(table_name)
        print(f'Таблица {table_name} содержит {count} строк.')
        chunk_size = CHUNK_SIZE
        start = 0
        while start < count:
            end = start + chunk_size
            if end > count:
                end = count
            print(f'забираем данные с {start} по {end} из {count}')
            data = sqlite_extractor.extract_data(
                start,
                end,
                table_name,
                data_class
            )
            postgres_saver.save_all_data(data, table_name)
            start = end
        print('данные выгружены и загружены')


if __name__ == '__main__':
    dsl = {
        'dbname': 'movies_database',
        'user': 'postgres',
        'password': 'postgres',
        'host': '127.0.0.1',
        'port': 5432
    }
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(
            **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
