from sqlalchemy.orm import Session
from .. import models, schemas
import datetime
import pytz
import json
from typing import Optional


# Get objects
def get_user_additional_info(db: Session, query: bool = False):
    if query:
        return db.query(models.UserAdditionalInfoOld)
    else:
        return db.query(models.UserAdditionalInfoOld).all()


def get_users(db: Session, query: bool = False):
    if query:
        return db.query(models.UserOld)
    else:
        return db.query(models.UserOld).all()


def get_teams(db: Session, query: bool = False):
    if query:
        return db.query(models.TeamOld)
    else:
        return db.query(models.TeamOld).all()


def get_participants(db: Session, query: bool = False):
    if query:
        return db.query(models.TeamParticipantsOld)
    else:
        return db.query(models.TeamParticipantsOld).all()


def get_submits(db: Session, query: bool = False):
    if query:
        return db.query(models.SubmitOld)
    else:
        return db.query(models.SubmitOld).all()
#
#
# def get_partners(db: Session, query: bool = False):
#     if query:
#         return db.query(models.Partner)
#     else:
#         return db.query(models.Partner).all()
#
#
# def get_hackathons(db: Session, query: bool = False):
#     if query:
#         return db.query(models.Hackathon)
#     else:
#         return db.query(models.Hackathon).all()


def create_submit(
        db: Session,
        submit: schemas.SubmitCreate
):
    new_submit = models.SubmitOld(
        **submit.dict(),
        submit_dt=datetime.datetime.now(pytz.timezone('Europe/Moscow')),
    )
    db.add(new_submit)
    db.commit()
    db.refresh(new_submit)

    return new_submit
#
#
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


def create_team(
        db: Session,
        team: schemas.TeamCreate
):
    new_team = models.TeamOld(**team.dict())
    db.add(new_team)
    db.commit()
    db.refresh(new_team)

    # logging(db=db, actions='create', object_name=new_user.__table__.name, params=user.dict())

    return new_team


def create_team_participant(
        db: Session,
        team_id: int,
        user_id: int,
        status: int,
):
    new_team = models.TeamParticipantsOld(team_id=team_id, user_id=user_id, status=status)
    db.add(new_team)
    db.commit()
    db.refresh(new_team)

    # logging(db=db, actions='create', object_name=new_user.__table__.name, params=user.dict())

    return new_team
#
#
def create_user_additional_info(
        db: Session,
        user_additional_info: schemas.UserAdditionalInfoBase
):
    new_user_additional_info = models.UserAdditionalInfoOld(**user_additional_info.dict())
    db.add(new_user_additional_info)
    db.commit()
    db.refresh(new_user_additional_info)

    return new_user_additional_info
#
#
# def create_hackathon(
#         db: Session,
#         hackathon: schemas.HackathonBase
# ):
#     new_hackathon = models.Hackathon(**hackathon.dict())
#     db.add(new_hackathon)
#     db.commit()
#     db.refresh(new_hackathon)
#
#     logging(db=db, actions='create', object_name=new_hackathon.__table__.name, params=hackathon.dict())
#
#     return new_hackathon
#
#
# def create_partner(
#         db: Session,
#         partner: schemas.PartnerBase
# ):
#     new_partner = models.Partner(**partner.dict())
#     db.add(new_partner)
#     db.commit()
#     db.refresh(new_partner)
#
#     logging(db=db, actions='create', object_name=new_partner.__table__.name, params=partner.dict())
#
#     return new_partner
#
