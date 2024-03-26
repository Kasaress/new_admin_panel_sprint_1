from dataclasses import InitVar, dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Base:
    id: str
    updated_at: InitVar[datetime] = None
    modified: datetime | None = None
    created: datetime | None = None
    created_at: InitVar[datetime] = None

    @staticmethod
    def _parse_datetime(dt_str: str) -> Optional[datetime]:
        try:
            return datetime.fromisoformat(dt_str)
        except ValueError:
            return None

    def __post_init__(self, created_at, updated_at, file_path=None):
        if created_at:
            self.created = created_at
        if updated_at:
            self.modified = updated_at
        if isinstance(self.modified, str):
            self.modified = self._parse_datetime(self.modified)
        if isinstance(self.created, str):
            self.created = self._parse_datetime(self.created)


@dataclass
class GenreData(Base):
    name: str = None
    description: str | None = None


@dataclass
class FilmworkData(Base):
    title: str = None
    type: str = None
    description: str | None = None
    creation_date: datetime | None = None
    rating: float | None = None
    file_path: InitVar[str] = None


@dataclass
class PersonData(Base):
    full_name: str = None


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
