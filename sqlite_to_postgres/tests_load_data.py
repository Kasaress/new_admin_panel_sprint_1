import sqlite3

import psycopg2
from psycopg2.extras import DictCursor

from constants import CHUNK_SIZE, DSL, MAPPING_TABLE_DATACLASS


class TestDataLoad:
    def __init__(self, sqlite_conn, pg_conn) -> None:
        self.sqlite_conn = sqlite_conn
        self.pg_conn = pg_conn

    def _get_sqlite_row_count(self, table_name: str) -> int:
        self.sqlite_conn.row_factory = sqlite3.Row
        cursor = self.sqlite_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        result = cursor.fetchone()
        return result[0]

    def _get_postgres_row_count(self, table_name: str) -> int:
        cursor = self.pg_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM content.{table_name}".format(table_name=table_name))
        result = cursor.fetchone()
        return result[0]

    def check_rows_counts(self, table_name: str) -> None:
        sqlite_row_count = self._get_sqlite_row_count(table_name)
        postgres_row_count = self._get_postgres_row_count(table_name)

        assert sqlite_row_count == postgres_row_count, (f'Не совпадает количество '
                                                        f'строк в таблицах {table_name}.'
                                                        f'В SQLite {sqlite_row_count},'
                                                        f'в Postgres {postgres_row_count}')
        print(f'количество строк в таблицах {table_name} совпадает')

    def run(self):
        for table_name, _ in MAPPING_TABLE_DATACLASS:
            self.check_rows_counts(table_name)


if __name__ == '__main__':
    print(f'Запуск тестов с конфигом: {DSL}')
    with (
        sqlite3.connect('db.sqlite') as sqlite_conn,
        psycopg2.connect(**DSL, cursor_factory=DictCursor) as pg_conn
    ):
        checker = TestDataLoad(sqlite_conn, pg_conn)
        checker.run()
