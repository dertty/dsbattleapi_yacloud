from sqlalchemy.orm import Session
from .. import models, schemas
import datetime
import pytz
import json
from typing import Optional


# Get objects
def get_user_additional_info(db: Session, query: bool = False):
    if query:
        return db.query(models.UserAdditionalInfo)
    else:
        return db.query(models.UserAdditionalInfo).all()


def get_users(db: Session, query: bool = False):
    if query:
        return db.query(models.User)
    else:
        return db.query(models.User).all()


def get_submits(db: Session, query: bool = False):
    if query:
        return db.query(models.Submit)
    else:
        return db.query(models.Submit).all()


def get_partners(db: Session, query: bool = False):
    if query:
        return db.query(models.Partner)
    else:
        return db.query(models.Partner).all()


def get_hackathons(db: Session, query: bool = False):
    if query:
        return db.query(models.Hackathon)
    else:
        return db.query(models.Hackathon).all()


# Create objects
def create_log(db: Session, log, uid: Optional[int] = None):
    log = models.Log(
        log=log,
        add_dt=datetime.datetime.now(pytz.timezone('Europe/Moscow')),
        uid=uid,
    )
    db.add(log)
    db.commit()

    return log


def logging(db: Session, actions: str, object_name: str, params: dict):
    log = {
        'action': actions,
        'object': object_name,
        'params': {**params}}
    json_log = json.dumps(log, separators=(',', ':'))
    create_log(db, json_log)


def create_submit(
        db: Session,
        submit: schemas.SubmitCreate
):
    new_submit = models.Submit(
        **submit.dict(),
        submit_dt=datetime.datetime.now(pytz.timezone('Europe/Moscow')),
    )
    db.add(new_submit)
    db.commit()
    db.refresh(new_submit)

    logging(db=db, actions='create', object_name=new_submit.__table__.name, params=submit.dict())

    return new_submit


def create_user(
        db: Session,
        user: schemas.UserBase
):
    new_user = models.User(**user.dict())
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    # logging(db=db, actions='create', object_name=new_user.__table__.name, params=user.dict())

    return new_user


def create_user_additional_info(
        db: Session,
        user_additional_info: schemas.UserAdditionalInfoBase
):
    new_user_additional_info = models.UserAdditionalInfo(
        **user_additional_info.dict(),
        add_dt=datetime.datetime.now(pytz.timezone('Europe/Moscow')),)
    db.add(new_user_additional_info)
    db.commit()
    db.refresh(new_user_additional_info)

    logging(db=db, actions='create', object_name=new_user_additional_info.__table__.name, params=user_additional_info.dict())

    return new_user_additional_info


def create_hackathon(
        db: Session,
        hackathon: schemas.HackathonBase
):
    new_hackathon = models.Hackathon(**hackathon.dict())
    db.add(new_hackathon)
    db.commit()
    db.refresh(new_hackathon)

    logging(db=db, actions='create', object_name=new_hackathon.__table__.name, params=hackathon.dict())

    return new_hackathon


def create_partner(
        db: Session,
        partner: schemas.PartnerBase
):
    new_partner = models.Partner(**partner.dict())
    db.add(new_partner)
    db.commit()
    db.refresh(new_partner)

    logging(db=db, actions='create', object_name=new_partner.__table__.name, params=partner.dict())

    return new_partner

