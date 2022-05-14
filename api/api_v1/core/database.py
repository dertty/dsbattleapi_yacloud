from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from .credentials import SQL_LOGIN, SQL_PASSWORD


SQLALCHEMY_DATABASE_URL = f'mysql+pymysql://{SQL_LOGIN}:{SQL_PASSWORD}@dsbatttle.cjf9z3vqvye8.us-east-2.rds.amazonaws.com/gb_bkhv'

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
