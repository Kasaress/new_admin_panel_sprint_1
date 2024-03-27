from dataclasses import astuple, fields

from config import SQL_INSERTS
from helpers import logger


class PostgresSaver:
    """Класс для сохранения данных в Postgres."""
    def __init__(self, connection):
        self.connection = connection

    @staticmethod
    def save_single_data(data, table_name, cursor) -> None:
        """Метод для загрузки одиночных значений в БД."""
        column_names = [field.name for field in fields(data)]
        column_names_str = ','.join(column_names)
        col_count = ', '.join(['%s'] * len(column_names))
        bind_values = cursor.mogrify(
            f"({col_count})", astuple(data)
        ).decode('utf-8')
        query = SQL_INSERTS.format(
            table_name=table_name,
            column_names_str=column_names_str,
            bind_values=bind_values
        )
        try:
            cursor.execute(query)
            logger.info(
                f'Строка вставлена в таблицу {table_name} - {bind_values}'
            )
        except Exception as error:
            logger.exception(
                f'Ошибка загрузки строки: {error} {bind_values=}.'
            )

    def save_all_data(self, data, table_name) -> None:
        """
        Метод пытается сохранить все переданные ему данные.
        В случае ошибки в чанке, метод пытается сохранить данные построчно.
        """
        logger.info(f'Старт загрузки данных в таблицу {table_name}.')
        with self.connection.cursor() as cursor:
            column_names = [field.name for field in fields(data[0])]
            column_names_str = ','.join(column_names)
            col_count = ', '.join(['%s'] * len(column_names))
            bind_values = ','.join(
                cursor.mogrify(
                    f"({col_count})", astuple(value)
                ).decode('utf-8') for value in data
            )
            query = SQL_INSERTS.format(
                table_name=table_name,
                column_names_str=column_names_str,
                bind_values=bind_values
            )
            try:
                cursor.executemany(query, bind_values)
                rows_inserted = cursor.rowcount
                logger.info(
                    f'{rows_inserted} из {len(data)} '
                    f'строк вставлено в таблицу {table_name}'
                )
            except Exception as error:
                logger.error(
                    f'Ошибка загрузки данных: {error}. '
                    'Старт повторной попытки загрузки данных.'
                )
                for value in data:
                    try:
                        self.save_single_data(value, table_name, cursor)
                    except Exception:
                        continue
