from datetime import datetime
from typing import Optional
from dataclasses import dataclass, field, InitVar

# @dataclass
#     class MyClass:
#         viewport: str = ""
#         vp: InitVar[str] = None
#
#         def __post_init__(self, vp):
#             if vp:
#                 self.viewport = vp
#
#     mc = MyClass(vp="foo")


@dataclass
class GenreData:
    id: str
    name: str
    updated_at: InitVar[datetime] = None
    modified: Optional[datetime] = None
    created: Optional[datetime] = None
    created_at: InitVar[datetime] = None
    description: Optional[str] = None

    def __post_init__(self, created_at, updated_at):
        if created_at:
            self.created = created_at
        if updated_at:
            self.modified = updated_at


@dataclass
class FilmworkData:
    id: str
    title: str
    description: str
    creation_date: datetime
    rating: float
    type: str
    genres: list[str]

@dataclass
class PersonData:
    id: str
    full_name: str


@dataclass
class PersonFilmworkData:
    id: str
    film_work_id: str
    person_id: str
    role: str
    created: datetime


@dataclass
class GenreFilmworkData:
    id: str
    film_work_id: str
    genre_id: str
    created: datetime
