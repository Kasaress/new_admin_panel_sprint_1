import sqlite3

import psycopg2
from psycopg2.extras import DictCursor

from constants import CHUNK_SIZE, DATACLASS_TYPE, DSL, MAPPING_TABLE_DATACLASS


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
        cursor.execute(
            "SELECT COUNT(*) "
            "FROM content.{table_name}".format(table_name=table_name)
        )
        result = cursor.fetchone()
        return result[0]

    def _get_sqlite_data(self, table_name, data_class, start_row, end_row):
        self.sqlite_conn.row_factory = sqlite3.Row
        cursor = self.sqlite_conn.cursor()
        query = f"SELECT * FROM {table_name} ORDER BY id LIMIT ?, ?"
        try:
            cursor.execute(query, (start_row, end_row - start_row))
            result = cursor.fetchall()
            return [data_class(**data) for data in result]
        except Exception as error:
            print(f'Ошибка чтения данных из таблицы {table_name}: {error}')
            return None

    def _get_postgres_data(self, table_name, data_class, start_row, end_row):
        cursor = self.pg_conn.cursor()
        query = ("SELECT * FROM content.{table_name} "
                 "ORDER BY id OFFSET %s "
                 "FETCH FIRST %s ROWS ONLY").format(table_name=table_name)

        try:
            cursor.execute(query, (start_row, end_row - start_row))
            result = cursor.fetchall()
            return [data_class(**data) for data in result]
        except Exception as error:
            print(f'Ошибка чтения данных из таблицы {table_name}: {error}')
            return None

    def check_rows_counts(self, table_name: str) -> None:
        sqlite_row_count = self._get_sqlite_row_count(table_name)
        postgres_row_count = self._get_postgres_row_count(table_name)

        assert sqlite_row_count == postgres_row_count, (
            f'Не совпадает количество '
            f'строк в таблицах {table_name}.'
            f'В SQLite {sqlite_row_count},'
            f'в Postgres {postgres_row_count}'
        )
        print(f'Количество строк в таблицах {table_name} совпадает.')

    def check_data_equal(
            self,
            table_name: str,
            data_class: DATACLASS_TYPE
    ) -> None:
        count = self._get_sqlite_row_count(table_name)
        chunk_size = CHUNK_SIZE
        start = 0
        while start < count:
            end = start + chunk_size
            if end > count:
                end = count
            sqlite_data = self._get_sqlite_data(
                table_name,
                data_class,
                start,
                end
            )
            postgres_data = self._get_postgres_data(
                table_name,
                data_class,
                start,
                end
            )
            assert sqlite_data == postgres_data, (
                f'Не совпадают данные в таблицах {table_name}. '
                f'{sqlite_data=}, '
                f'{postgres_data=}.'
            )
            start = end

        print(f'Данные в таблицах {table_name} совпадают.')

    def run(self):
        for table_name, data_class in MAPPING_TABLE_DATACLASS:
            self.check_rows_counts(table_name)
            self.check_data_equal(table_name, data_class)


if __name__ == '__main__':
    print(f'Запуск тестов с конфигом: {DSL}')
    with (
        sqlite3.connect('db.sqlite') as sqlite_conn,
        psycopg2.connect(
            **DSL, cursor_factory=DictCursor
        ) as pg_conn  # type: ignore
    ):
        checker = TestDataLoad(sqlite_conn, pg_conn)
        checker.run()
