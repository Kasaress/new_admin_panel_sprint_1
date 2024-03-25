import os

from dotenv import load_dotenv

from data_objects import (FilmworkData, GenreData, GenreFilmworkData,
                          PersonData, PersonFilmworkData)

load_dotenv()

MAPPING_TABLE_DATACLASS = [
    ('genre', GenreData),
    ('person', PersonData),
    ('film_work', FilmworkData),
    ('person_film_work', PersonFilmworkData),
    ('genre_film_work', GenreFilmworkData),
]

CHUNK_SIZE = 10

SQL_INSERTS = """
        INSERT INTO content.{table_name}
        ({column_names_str}) VALUES {bind_values}
        ON CONFLICT (id) DO NOTHING
      """

DSL = {
    'dbname': os.environ.get('DBNAME', 'movies_database'),
    'user': os.environ.get('PG_USER', 'postgres'),
    'password': os.environ.get('PASSWORD', 'postgres'),
    'host': os.environ.get('HOST', '127.0.0.1'),
    'port': os.environ.get('PORT', 5432)
}
