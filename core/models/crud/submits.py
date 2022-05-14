from sqlalchemy import func
from sqlalchemy.orm import Session, contains_eager, load_only
from .. import models
from ..crud import crud
from sqlalchemy import Integer
from typing import Optional


def get_leaderboard(
    db: Session,
    hid: int,
    bid: int,
    uid: Optional[int] = None,
):
    if uid is None:
        leaderboard = crud.get_hackathons(db=db, query=True) \
            .filter(models.Hackathon.id == hid) \
            .filter(models.Submit.bid == bid) \
            .options(contains_eager(models.Hackathon.submits)) \
            .order_by(models.Submit.public_score.desc(), models.Submit.submit_dt.desc()) \
            .with_entities(
                func.row_number().over().label('rank'),
                models.User.username,
                models.Submit.public_score, ) \
            .all()
        return leaderboard
    else:
        user_submits = crud.get_hackathons(db=db, query=True) \
            .filter(models.Hackathon.id == hid) \
            .filter(models.Submit.bid == bid) \
            .filter(models.Submit.uid == uid) \
            .options(contains_eager(models.Hackathon.submits)) \
            .order_by(models.Submit.submit_dt.desc(), models.Submit.public_score.desc()) \
            .with_entities(
                models.Submit.id,
                func.row_number().over().label('rank'),
                models.User.username,
                models.Submit.public_score,
                models.Submit.submit_dt,
                models.Submit.stared_flg,
                models.Submit.comment,) \
            .all()

    return user_submits


def star_submit_star_flag(db: Session, sid: int):
    edited_submit = crud.get_submits(db=db, query=True) \
        .filter(models.Submit.id == sid) \
        .options(
            load_only('public_score', 'stared_flg', 'comment', 'file_location',)) \
        .first()
    edited_submit.stared_flg = True
    db.commit()
    db.refresh(edited_submit)

    crud.logging(db=db, actions='change', object_name=edited_submit.__table__.name, params={'sid': sid, 'stared_flg': True})

    return edited_submit


def unstar_submit_star_flag(db: Session, sid: int):
    edited_submit = crud.get_submits(db=db, query=True) \
        .filter(models.Submit.id == sid) \
        .options(
            load_only('public_score', 'stared_flg', 'comment', 'file_location',)) \
        .first()
    edited_submit.stared_flg = False
    db.commit()
    db.refresh(edited_submit)

    crud.logging(db=db, actions='change', object_name=edited_submit.__table__.name, params={'sid': sid, 'stared_flg': False})

    return edited_submit


def count_submit_star_flags(db: Session, uid: int):
    return crud.get_submits(db=db, query=True) \
        .filter(models.Submit.uid == uid) \
        .with_entities(func.sum(models.Submit.stared_flg.cast(Integer)).label('flags_num')) \
        .first().flags_num



