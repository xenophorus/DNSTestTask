from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from settings import settings

engine = create_engine(
    url=settings.get_url(),
    echo=False
)

session = sessionmaker(engine)


class Base(DeclarativeBase):
    pass
