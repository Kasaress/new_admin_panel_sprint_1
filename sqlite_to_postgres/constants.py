from data_objects import (FilmworkData, GenreData, GenreFilmworkData,
                          PersonData, PersonFilmworkData)

MAPPING_TABLE_DATACLASS = [
    ('genre', GenreData),
    # ('person', PersonData),
    # ('film_work', FilmworkData),
    # ('person_film_work', PersonFilmworkData),
    # ('genre_film_work', GenreFilmworkData),
]

CHUNK_SIZE = 10

SQL_INSERTS = """
        INSERT INTO content.{table_name} 
        ({column_names_str}) VALUES {bind_values} 
        ON CONFLICT (id) DO NOTHING
      """