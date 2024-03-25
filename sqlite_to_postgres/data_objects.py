from dataclasses import InitVar, dataclass
from datetime import datetime


@dataclass
class GenreData:
    id: str
    name: str
    updated_at: InitVar[datetime] = None
    modified: datetime | None = None
    created: datetime | None = None
    created_at: InitVar[datetime] = None
    description: str | None = None

    def __post_init__(self, created_at, updated_at):
        if created_at:
            self.created = created_at
        if updated_at:
            self.modified = updated_at


@dataclass
class FilmworkData:
    id: str
    title: str
    type: str
    description: str | None = None
    creation_date: datetime | None = None
    rating: float | None = None
    file_path: InitVar[str] = None
    updated_at: InitVar[datetime] = None
    modified: datetime | None = None
    created: datetime | None = None
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
    modified: datetime | None = None
    created: datetime | None = None
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
    created: datetime | None = None
    created_at: InitVar[datetime] = None

    def __post_init__(self, created_at):
        if created_at:
            self.created = created_at


@dataclass
class GenreFilmworkData:
    id: str
    film_work_id: str
    genre_id: str
    created: datetime | None = None
    created_at: InitVar[datetime] = None

    def __post_init__(self, created_at):
        if created_at:
            self.created = created_at
