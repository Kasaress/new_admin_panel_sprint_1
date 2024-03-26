from dataclasses import InitVar, dataclass
from datetime import datetime


@dataclass
class Created:
    created: datetime | None = None
    created_at: InitVar[datetime] = None

    @staticmethod
    def _parse_datetime(dt_str: str) -> datetime | None:
        try:
            return datetime.fromisoformat(dt_str)
        except ValueError:
            return None

    def __post_init__(self, created_at):
        if created_at:
            self.created = created_at
        if isinstance(self.created, str):
            self.created = self._parse_datetime(self.created)


@dataclass
class Base:
    id: str
    updated_at: InitVar[datetime] = None
    modified: datetime | None = None
    created: datetime | None = None
    created_at: InitVar[datetime] = None

    @staticmethod
    def _parse_datetime(dt_str: str) -> datetime | None:
        try:
            return datetime.fromisoformat(dt_str)
        except ValueError:
            return None

    def __post_init__(self, created_at, updated_at):
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
class PersonData(Base):
    full_name: str = None


@dataclass
class PersonFilmworkData(Created):
    id: str = None
    film_work_id: str = None
    person_id: str = None
    role: str = None


@dataclass
class GenreFilmworkData(Created):
    id: str = None
    film_work_id: str = None
    genre_id: str = None


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

    @staticmethod
    def _parse_datetime(dt_str: str) -> datetime | None:
        try:
            return datetime.fromisoformat(dt_str)
        except ValueError:
            return None

    def __post_init__(self, file_path, updated_at, created_at):
        if created_at:
            self.created = created_at
        if updated_at:
            self.modified = updated_at
        if isinstance(self.modified, str):
            self.modified = self._parse_datetime(self.modified)
        if isinstance(self.created, str):
            self.created = self._parse_datetime(self.created)
