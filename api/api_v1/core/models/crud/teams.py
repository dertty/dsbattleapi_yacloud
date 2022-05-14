from sqlalchemy import func, or_
from sqlalchemy.orm import Session, contains_eager, load_only
from .. import models, schemas
from ..crud import crud
import datetime


def get_team(db: Session, uid: int):
    owner = crud.get_teams(db=db, query=True) \
        .filter(models.TeamOld.active == 1) \
        .filter(models.TeamOld.owner_id == uid) \
        .all()

    if len(owner) > 0:
        send_invites = crud.get_participants(db=db, query=True) \
            .filter(models.TeamParticipantsOld.team_id == owner[0].id) \
            .filter(or_(models.TeamParticipantsOld.status == 0, models.TeamParticipantsOld.status == 1)) \
            .join(models.UserOld,  models.TeamParticipantsOld.user_id == models.UserOld.id) \
            .with_entities(
                models.UserOld.name.label('user_name'),
                models.UserOld.email.label('email'),
                models.TeamParticipantsOld.status,
                models.TeamParticipantsOld.id,
            ) \
            .all()
    else:
        send_invites = []

    teams_in = crud.get_teams(db=db, query=True) \
        .filter(models.TeamOld.active == 1) \
        .subquery()

    participants = crud.get_participants(db=db, query=True) \
        .filter(models.TeamParticipantsOld.user_id == uid) \
        .filter(models.TeamParticipantsOld.status == 1) \
        .join(teams_in, models.TeamParticipantsOld.team_id == teams_in.c.id) \
        .join(models.UserOld, models.UserOld.id == teams_in.c.owner_id) \
        .with_entities(
            models.UserOld.name.label('user_name'),
            models.UserOld.email.label('email'),
            models.TeamParticipantsOld.status,
            models.TeamParticipantsOld.id,
            teams_in.c.name,
        ) \
        .all()

    invites = crud.get_participants(db=db, query=True) \
        .filter(models.TeamParticipantsOld.user_id == uid) \
        .filter(models.TeamParticipantsOld.status == 0) \
        .all()

    approved_teammates = crud.get_participants(db=db, query=True) \
        .filter(models.TeamParticipantsOld.status == 1) \
        .subquery()

    teammates_info = crud.get_teams(db=db, query=True) \
        .filter(models.TeamOld.active == 1) \
        .filter(models.TeamOld.id == uid)\
        .join(approved_teammates, models.TeamOld.id == approved_teammates.c.team_id) \
        .join(models.UserOld, models.UserOld.id == approved_teammates.c.user_id) \
        .with_entities(
            models.UserOld.name.label('user_name'),
            models.TeamOld.name.label('team_name'),
            models.UserOld.name.label('cp_desc'),
        ).all()

    return {
        'owner': owner,
        'owner_invites': [dict(zip(('user_name', 'email', 'status', 'id',), x)) for x in send_invites if x[2] == 0],
        'approved_invites': [dict(zip(('user_name', 'email', 'status', 'id',), x)) for x in send_invites if x[2] == 1],
        'invites': [
            {
                'team_description': crud.get_teams(db=db, query=True).filter(models.TeamOld.id == x.team_id).first().name,
                'team_id': x.id,
                'id': x.id
            } for x in invites
        ],
        'participant': [dict(zip(('owner', 'email', 'status', 'id', 'name'), x)) for x in participants],
        'cp': teammates_info if len(owner) == 0 else [dict(zip(('cp_desc', 'email', 'status', 'id',), x)) for x in send_invites if x[2] == 1],
    }
    # {
    #     'owner': owner,
    #     'invites': [
    #         {'id': 21211, 'team_description': 'allstars1'},
    #         {'id': 21212, 'team_description': 'allstars2'},
    #         {'id': 21213, 'team_description': 'allstars3'}
    #     ],
    #     'participant': participants,
    #     'cp': [
    #         {'id': 21211, 'name': 'allstars1', 'email': 'sasha'},
    #     ],
    #
    # }


# def get_participants(db: Session, login_credentials: schemas.UserLogin,):
#     return crud.get_users(db=db, query=True) \
#         .filter(func.lower(models.User.username) == func.lower(login_credentials.username)) \
#         .all()
#
#
# def change_name(db, uid, name):
#     user = crud.get_users(db=db, query=True).get(uid)
#     setattr(user, 'name', name)
#     db.commit()
#     db.flush()