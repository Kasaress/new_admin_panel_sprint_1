from datetime import datetime
from typing import Optional
from dataclasses import dataclass, InitVar, field


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
    file_path: InitVar[str] = None
    updated_at: InitVar[datetime] = None
    modified: Optional[datetime] = None
    created: Optional[datetime] = None
    created_at: InitVar[datetime] = None

    def __post_init__(self, file_path, updated_at, created_at):
        if created_at:
            self.created = created_at
        if updated_at:
            self.modified = updated_at


@dataclass
class PersonData:
    id: str
    full_name: str
    updated_at: InitVar[datetime] = None
    modified: Optional[datetime] = None
    created: Optional[datetime] = None
    created_at: InitVar[datetime] = None

    def __post_init__(self, created_at, updated_at):
        if created_at:
            self.created = created_at
        if updated_at:
            self.modified = updated_at



@dataclass
class PersonFilmworkData:
    id: str
    film_work_id: str
    person_id: str
    role: str
    created: Optional[datetime] = None
    created_at: InitVar[datetime] = None

    def __post_init__(self, created_at):
        if created_at:
            self.created = created_at

@dataclass
class GenreFilmworkData:
    id: str
    film_work_id: str
    genre_id: str
    created: Optional[datetime] = None
    created_at: InitVar[datetime] = None

    def __post_init__(self, created_at):
        if created_at:
            self.created = created_at
