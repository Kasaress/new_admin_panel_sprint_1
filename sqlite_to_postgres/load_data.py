import sqlite3

import psycopg2
from psycopg2.extras import DictCursor
from data_objects import GenreData
from dataclasses import fields, astuple


class PostgresSaver:
    def __init__(self, conn):
        self.conn = conn

    def save_all_data(self, data):
        cursor = self.conn.cursor()
        column_names = [field.name for field in fields(data[0])]
        column_names_str = ','.join(column_names)

        col_count = ', '.join(['%s'] * len(column_names))

        bind_values = ','.join(cursor.mogrify(f"({col_count})", astuple(dat)).decode('utf-8') for dat in data)

        query = (f'INSERT INTO content.genre ({column_names_str}) VALUES {bind_values} '
                 f' ON CONFLICT (id) DO NOTHING'
                 )

        cursor.execute(query)
        print('я все сохранил')

class SQLiteExtractor:
    def __init__(self, conn):
        self.conn = conn

    def get_count_rows(self):

        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM genre")

        result = cursor.fetchone()
        return result[0]

    def extract_movies(self, start_row, end_row):  # TODO контекстный менеджер
        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()
        # cursor.execute("""SELECT * FROM genre""")
        cursor.execute("""SELECT * FROM genre LIMIT ?, ?""", (start_row, end_row - start_row))

        result = cursor.fetchall()
        print('я все извлек')

        return [GenreData(**data) for data in result]


def chunks(iterable, n):
    for i in range(0, len(iterable), n):
        yield iterable[i:i+n]


def load_from_sqlite(connection: sqlite3.Connection, pg_conn):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)
    # для списка таблиц извлеки данные и сохрани их.
    # в параметры возьми имя таблицы и внутри из маппинга в константе достать нужный класс
    count = sqlite_extractor.get_count_rows()
    chunk_size = 50
    start = 0
    while start < count:
        end = start + chunk_size
        if end > count:
            end = count
        data = sqlite_extractor.extract_movies(start, end)
        print(data)
        postgres_saver.save_all_data(data)
        start = end




if __name__ == '__main__':
    dsl = {'dbname': 'movies_database', 'user': 'postgres', 'password': 'postgres', 'host': '127.0.0.1', 'port': 5432}
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
