import sqlite3

import psycopg2
from psycopg2.extras import DictCursor
from data_objects import GenreData, PersonData, FilmworkData, PersonFilmworkData, GenreFilmworkData
from dataclasses import fields, astuple, asdict
import traceback



class PostgresSaver:
    def __init__(self, conn):
        self.conn = conn

    def save_all_data(self, data, table):
        cursor = self.conn.cursor()
        column_names = [field.name for field in fields(data[0])]
        column_names_str = ','.join(column_names)

        col_count = ', '.join(['%s'] * len(column_names))

        bind_values = ','.join(cursor.mogrify(f"({col_count})", astuple(dat)).decode('utf-8') for dat in data)
        print(f'{bind_values=}')
        query = (f'INSERT INTO content.{table} ({column_names_str}) VALUES {bind_values} '
                 f' ON CONFLICT (id) DO NOTHING'
                 )
        try:
            cursor.executemany(query, bind_values)
            rows_inserted = cursor.rowcount
            rows_not_inserted = len(data) - rows_inserted
            print(f'{rows_inserted} rows inserted to {table}')
            print(f'{rows_not_inserted} rows not inserted to {table}')
        except ValueError as error:
            print(f'!!!! Ошибка загрузки данных: {error}')
            traceback.print_exc()


class SQLiteExtractor:
    def __init__(self, conn):
        self.conn = conn

    def get_count_rows(self, table_name):
        """Возвращает количество строк в переданной таблице."""

        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")

        result = cursor.fetchone()
        return result[0]

    def extract_data(self, start_row, end_row, table_name, data_class):  # TODO контекстный менеджер
        """
        Универсальный метод извлечения данных из БД.
        Принимает название таблицы для выгрузки и датакласс для преобразования и валидации данных.
        Осуществляет выгрузку чанками.
        """
        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()

        query = f"SELECT * FROM {table_name} LIMIT ?, ?"
        cursor.execute(query, (start_row, end_row - start_row))

        result = cursor.fetchall()
        res = [data_class(**data) for data in result]
        print(f'raw {res=}')
        # print(f'{res[0].created=}')
        return res

mapping = [
    # ('genre', GenreData),
    # ('person', PersonData),
    ('film_work', FilmworkData),
    # ('person_film_work', PersonFilmworkData),
    # ('genre_film_work', GenreFilmworkData),
]
def load_from_sqlite(connection: sqlite3.Connection, pg_conn):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)

    for table_name, data_class in mapping:
        print(table_name, data_class)
        count = sqlite_extractor.get_count_rows(table_name)
        print(f'достали из бд {count} строк')
        chunk_size = 100
        start = 0
        while start < count:
            end = start + chunk_size
            if end > count:
                end = count
            print(f'забираем данные с {start} по {end} из {count}')
            data = sqlite_extractor.extract_data(start, end, table_name, data_class)
            postgres_saver.save_all_data(data, table_name)
            start = end
        print('данные выгружены и загружены')


if __name__ == '__main__':
    dsl = {'dbname': 'movies_database', 'user': 'postgres', 'password': 'postgres', 'host': '127.0.0.1', 'port': 5432}
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
