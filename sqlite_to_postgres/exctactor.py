import sqlite3


class SQLiteExtractor:
    """Класс для выгрузки данных из SQLite."""
    def __init__(self, connection):
        self.connection = connection

    def get_count_rows(self, table_name):
        """Возвращает количество строк в переданной таблице."""
        self.connection.row_factory = sqlite3.Row
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            result = cursor.fetchone()
            return result[0]
        except Exception as error:
            print(f'Ошибка чтения данных из таблицы {table_name}: {error}')
            return None

    def extract_data(self, start_row, end_row, table_name, data_class):
        """
        Универсальный метод извлечения данных из БД.
        Принимает название таблицы для выгрузки и
        датакласс для преобразования и валидации данных.
        Осуществляет выгрузку чанками.
        """
        self.connection.row_factory = sqlite3.Row
        cursor = self.connection.cursor()
        query = f"SELECT * FROM {table_name} LIMIT ?, ?"
        try:
            cursor.execute(query, (start_row, end_row - start_row))
            result = cursor.fetchall()
            print(
                f'Данные получены из SQLite: таблица {table_name}, '
                f'строки с {start_row} по {end_row}'
            )
            return [data_class(**data) for data in result]
        except Exception as error:
            print(f'Ошибка чтения данных из таблицы {table_name}: {error}')
            return None
