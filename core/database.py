from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from .credentials import SQL_LOGIN, SQL_PASSWORD


SQLALCHEMY_DATABASE_URL = f'mysql+pymysql://{SQL_LOGIN}:{SQL_PASSWORD}@dsbattle.ci8kn75uivdu.eu-central-1.rds.amazonaws.com/hackathons'

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
