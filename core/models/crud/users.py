from sqlalchemy import func
from sqlalchemy.orm import Session, contains_eager, load_only
from .. import models, schemas
from ..crud import crud


def get_emails(db: Session, login_credentials: schemas.UserLogin,):
    return crud.get_users(db=db, query=True) \
        .filter(func.lower(models.User.email) == func.lower(login_credentials.email)) \
        .all()


def get_usernames(db: Session, login_credentials: schemas.UserLogin,):
    return crud.get_users(db=db, query=True) \
        .filter(func.lower(models.User.username) == func.lower(login_credentials.username)) \
        .all()
