import sqlite3

import psycopg2
from psycopg2.extras import DictCursor

from constants import CHUNK_SIZE, DSL, MAPPING_TABLE_DATACLASS
from exctactor import SQLiteExtractor
from loader import PostgresSaver


def load_from_sqlite(connection: sqlite3.Connection, pg_conn):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)

    for table_name, data_class in MAPPING_TABLE_DATACLASS:
        print(f'Старт копирования данных из таблицы {table_name}.')
        count = sqlite_extractor.get_count_rows(table_name)
        if not count:
            print(f'Таблица {table_name} не содержит данных.')
            continue
        print(f'Таблица {table_name} содержит {count} строк.')
        chunk_size = CHUNK_SIZE
        start = 0
        while start < count:
            end = start + chunk_size
            if end > count:
                end = count
            print(
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
                print(f'Не получены данные из таблицы {table_name}.')
                continue
            print(
                f'Старт сохранения данных с {start} '
                f'по {end} строки из {count} строк из таблицы {table_name}.'
            )
            postgres_saver.save_all_data(data, table_name)
            start = end
        print(f'Финиш копирования данных из таблицы {table_name}.')
    print('Все данные из всех таблиц обработаны.')


if __name__ == '__main__':
    print(f'Запуск скрипта с конфигом: {DSL}')
    with (
        sqlite3.connect('db.sqlite') as sqlite_conn,
        psycopg2.connect(**DSL, cursor_factory=DictCursor) as pg_conn  # type: ignore
    ):
        load_from_sqlite(sqlite_conn, pg_conn)
