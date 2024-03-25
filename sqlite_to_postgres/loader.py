from dataclasses import astuple, fields


class PostgresSaver:
    """Класс для сохранения данных в Postgres."""
    def __init__(self, conn):
        self.conn = conn

    @staticmethod
    def save_single_data(data, table_name, cursor):
        """Метод для загрузки одиночных значений в БД."""
        column_names = [field.name for field in fields(data)]
        column_names_str = ','.join(column_names)
        col_count = ', '.join(['%s'] * len(column_names))
        bind_values = cursor.mogrify(
            f"({col_count})", astuple(data)
        ).decode('utf-8')
        query = (
            f'INSERT INTO content.{table_name} '
            f'({column_names_str}) VALUES {bind_values} '
            f' ON CONFLICT (id) DO NOTHING'
        )
        try:
            cursor.execute(query)
            print(f'Single row inserted to {table_name} - {bind_values}')
        except Exception as error:
            print(f'Ошибка загрузки значения: {error} {bind_values=}.')

    def save_all_data(self, data, table_name):
        """
        Метод пытается сохранить все переданные ему данные.
        В случае ошибки в чанке, метод пытается сохранить данные построчно.
        """
        print(f'Старт загрузки данных в таблицу {table_name}.')
        cursor = self.conn.cursor()
        column_names = [field.name for field in fields(data[0])]
        column_names_str = ','.join(column_names)
        col_count = ', '.join(['%s'] * len(column_names))
        bind_values = ','.join(
            cursor.mogrify(
                f"({col_count})", astuple(value)
            ).decode('utf-8') for value in data
        )
        query = (
            f'INSERT INTO content.{table_name} '
            f'({column_names_str}) VALUES {bind_values} '
            f' ON CONFLICT (id) DO NOTHING'
        )
        try:
            cursor.executemany(query, bind_values)
            rows_inserted = cursor.rowcount
            rows_not_inserted = len(data) - rows_inserted
            print(f'{rows_inserted} rows inserted to {table_name}')
            print(f'{rows_not_inserted} rows not inserted to {table_name}')
        except Exception as error:
            print(
                f'Ошибка загрузки данных: {error}. '
                'Старт повторной попытки загрузки данных.'
            )
            for value in data:
                self.save_single_data(value, table_name, cursor)
