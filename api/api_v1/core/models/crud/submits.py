from sqlalchemy import func, or_, distinct, union, desc
from sqlalchemy.orm import Session, contains_eager, load_only
from sqlalchemy.dialects.postgresql import array_agg
from .. import models
from ..crud import crud
from sqlalchemy import Integer
from typing import Optional
import datetime
import pytz
# import pandas as pd
import numpy as np


def get_leaderboard(
    db: Session,
    hid: int,
    bid: int,
    uid: Optional[int] = None,
    org: Optional[str] = '',
):
    if uid is None:
        if hid == 3:
            if bid == 2:
                team_participant = crud.get_teams(db=db, query=True) \
                    .join(models.TeamParticipantsOld, models.TeamParticipantsOld.team_id == models.TeamOld.id, isouter=True) \
                    .filter(models.TeamOld.active == 1) \
                    .filter(or_(models.TeamParticipantsOld.status == 1, models.TeamParticipantsOld.status == None)) \
                    .with_entities(
                        models.TeamOld.name,
                        models.TeamOld.owner_id,
                        models.TeamParticipantsOld.user_id,
                    ) \
                    .subquery()

                teams_submits_1 = crud.get_submits(db=db, query=True) \
                    .join(team_participant, models.SubmitOld.uid == team_participant.c.owner_id) \
                    .filter(models.SubmitOld.hid == hid) \
                    .filter(models.SubmitOld.bid == bid) \
                    .with_entities(
                        team_participant.c.name,
                        models.SubmitOld.public_score,
                    )
                teams_submits_2 = crud.get_submits(db=db, query=True) \
                    .join(team_participant, models.SubmitOld.uid == team_participant.c.user_id) \
                    .filter(models.SubmitOld.hid == hid) \
                    .filter(models.SubmitOld.bid == bid) \
                    .with_entities(
                        team_participant.c.name,
                        models.SubmitOld.public_score,
                    )
                teams_submits = teams_submits_1.union(teams_submits_2).all()
                teams_submits_list = [[x.name, x.public_score] for x in teams_submits]
                result = {}
                for l in teams_submits_list:
                    if result.get(l[0], None) is None:
                        result[l[0]] = l[1]
                    elif result.get(l[0]) < l[1]:
                        result[l[0]] = l[1]
                    else:
                        pass
                return [dict(zip(['rank', 'name', 'public_score'], [x[0] + 1, x[1][0], x[1][1]])) for x in enumerate(sorted(result.items(), key=lambda k: -k[1]))]
            if bid == 1:
                team_participant = crud.get_teams(db=db, query=True) \
                    .join(models.TeamParticipantsOld, models.TeamParticipantsOld.team_id == models.TeamOld.id, isouter=True) \
                    .filter(models.TeamOld.active == 1) \
                    .filter(or_(models.TeamParticipantsOld.status == 1, models.TeamParticipantsOld.status == None)) \
                    .with_entities(
                        models.TeamOld.name,
                        models.TeamOld.owner_id,
                        models.TeamParticipantsOld.user_id,
                    ) \
                    .subquery()

                teams_submits_1 = crud.get_submits(db=db, query=True) \
                    .join(team_participant, models.SubmitOld.uid == team_participant.c.owner_id) \
                    .filter(models.SubmitOld.hid == hid) \
                    .filter(models.SubmitOld.bid == bid) \
                    .with_entities(
                        team_participant.c.name,
                        models.SubmitOld.public_score,
                        models.SubmitOld.public_score_1,
                    )
                teams_submits_2 = crud.get_submits(db=db, query=True) \
                    .join(team_participant, models.SubmitOld.uid == team_participant.c.user_id) \
                    .filter(models.SubmitOld.hid == hid) \
                    .filter(models.SubmitOld.bid == bid) \
                    .with_entities(
                        team_participant.c.name,
                        models.SubmitOld.public_score,
                        models.SubmitOld.public_score_1,
                    )
                teams_submits = teams_submits_1.union(teams_submits_2).all()
                teams_submits_list = [[x.name, x.public_score, x.public_score_1, x.public_score + x.public_score_1, str(round(x.public_score, 3)) + '/' + str(round(x.public_score_1, 3))] for x in teams_submits]
                result = {}
                for l in teams_submits_list:
                    if result.get(l[0], None) is None:
                        result[l[0]] = (l[3], l[4])
                    elif result.get(l[0])[0] < l[3]:
                        result[l[0]] = (l[3], l[4])
                    else:
                        pass
                return [dict(zip(['rank', 'name', 'public_score'], [x[0] + 1, x[1][0], x[1][1][1]])) for x in enumerate(sorted(result.items(), key=lambda k: -k[1][0]))]
        elif hid == 7:
            sort_rank = func.rank().over(partition_by=models.SubmitOld.uid,
                                         order_by=[models.SubmitOld.public_score.asc(),
                                                   models.SubmitOld.submit_dt.asc()]).label('sort_rank')
            pre_leaderboard = crud.get_submits(db=db, query=True) \
                .filter(models.SubmitOld.hid == hid) \
                .filter(models.SubmitOld.bid == bid) \
                .with_entities(
                    models.SubmitOld.uid,
                    models.SubmitOld.public_score,
                    sort_rank,
                ) \
                .subquery('t1')
            if len(org) > 0:
                leaderboard = crud.get_users(db=db, query=True) \
                    .join(pre_leaderboard, models.UserOld.id == pre_leaderboard.c.uid) \
                    .filter(pre_leaderboard.c.sort_rank == 1) \
                    .filter(models.UserOld.org == org) \
                    .order_by(pre_leaderboard.c.public_score.asc()) \
                    .with_entities(
                        func.row_number().over().label('rank'),
                        models.UserOld.name,
                        func.concat(func.round(pre_leaderboard.c.public_score, 4)).label('public_score'),
                    ) \
                    .all()
            else:
                leaderboard = crud.get_users(db=db, query=True) \
                    .join(pre_leaderboard, models.UserOld.id == pre_leaderboard.c.uid) \
                    .filter(pre_leaderboard.c.sort_rank == 1) \
                    .order_by(pre_leaderboard.c.public_score.asc()) \
                    .with_entities(
                        func.row_number().over().label('rank'),
                        models.UserOld.name,
                        func.concat(func.round(pre_leaderboard.c.public_score, 4)).label('public_score'),
                    ) \
                    .all()
            return leaderboard
        sort_rank = func.rank().over(
            partition_by=models.SubmitOld.uid,
            order_by=[models.SubmitOld.public_score.desc(),
                      models.SubmitOld.submit_dt.asc()]).label('sort_rank')
        pre_leaderboard = crud.get_submits(db=db, query=True) \
            .filter(models.SubmitOld.hid == hid) \
            .filter(models.SubmitOld.bid == bid) \
            .with_entities(
                models.SubmitOld.uid,
                models.SubmitOld.public_score,
                sort_rank,
            ) \
            .subquery('t1')
        if len(org) > 0:
            leaderboard = crud.get_users(db=db, query=True) \
                .join(pre_leaderboard, models.UserOld.id == pre_leaderboard.c.uid) \
                .filter(pre_leaderboard.c.sort_rank == 1) \
                .filter(models.UserOld.org == org) \
                .order_by(pre_leaderboard.c.public_score.desc()) \
                .with_entities(
                    func.row_number().over().label('rank'),
                    models.UserOld.name,
                    func.concat(func.round(pre_leaderboard.c.public_score, 3)).label('public_score'),
                ) \
                .all()
        else:
            leaderboard = crud.get_users(db=db, query=True) \
                .join(pre_leaderboard, models.UserOld.id == pre_leaderboard.c.uid) \
                .filter(pre_leaderboard.c.sort_rank == 1) \
                .order_by(pre_leaderboard.c.public_score.desc()) \
                .with_entities(
                    func.row_number().over().label('rank'),
                    models.UserOld.name,
                    func.concat(func.round(pre_leaderboard.c.public_score, 3)).label('public_score'),
                ) \
                .all()
        return leaderboard
    else:
        if hid == 3:
            if bid == 1 or bid == 2:
                team_participant = crud.get_teams(db=db, query=True) \
                    .join(models.TeamParticipantsOld, models.TeamParticipantsOld.team_id == models.TeamOld.id, isouter=True) \
                    .filter(models.TeamOld.active == 1) \
                    .filter(or_(models.TeamParticipantsOld.status == 1, models.TeamParticipantsOld.status == None)) \
                    .filter(or_(models.TeamParticipantsOld.user_id == uid, models.TeamOld.owner_id == uid)) \
                    .with_entities(
                        models.TeamOld.owner_id,
                        models.TeamParticipantsOld.user_id,
                    ) \
                    .all()
                if len(team_participant) == 0:
                    user_submits = crud.get_submits(db=db, query=True) \
                        .filter(models.SubmitOld.hid == hid) \
                        .filter(models.SubmitOld.bid == bid) \
                        .filter(models.SubmitOld.uid == uid) \
                        .order_by(models.SubmitOld.submit_dt.desc(), models.SubmitOld.public_score.desc()) \
                        .with_entities(
                            models.SubmitOld.id,
                            func.row_number().over().label('rank'),
                            models.SubmitOld.public_score,
                            models.SubmitOld.submit_dt,
                            models.SubmitOld.stared_flg,
                            models.SubmitOld.comment,
                            models.SubmitOld.file_location) \
                        .all()
                else:
                    user_submits = crud.get_submits(db=db, query=True) \
                        .filter(models.SubmitOld.hid == hid) \
                        .filter(models.SubmitOld.bid == bid) \
                        .filter(or_(models.SubmitOld.uid == team_participant[0].owner_id, models.SubmitOld.uid == team_participant[0].user_id)) \
                        .order_by(models.SubmitOld.submit_dt.desc(), models.SubmitOld.public_score.desc()) \
                        .with_entities(
                            models.SubmitOld.id,
                            func.row_number().over().label('rank'),
                            models.SubmitOld.public_score,
                            models.SubmitOld.submit_dt,
                            models.SubmitOld.stared_flg,
                            models.SubmitOld.comment,
                            models.SubmitOld.file_location
                        ) \
                        .all()
            else:
                user_submits = crud.get_submits(db=db, query=True) \
                    .filter(models.SubmitOld.hid == hid) \
                    .filter(models.SubmitOld.bid == bid) \
                    .filter(models.SubmitOld.uid == uid) \
                    .order_by(models.SubmitOld.submit_dt.desc(), models.SubmitOld.public_score.desc()) \
                    .with_entities(
                    models.SubmitOld.id,
                    func.row_number().over().label('rank'),
                    models.SubmitOld.public_score,
                    models.SubmitOld.submit_dt,
                    models.SubmitOld.stared_flg,
                    models.SubmitOld.comment,
                    models.SubmitOld.file_location) \
                    .all()
        elif hid == 7:
            user_submits = crud.get_submits(db=db, query=True) \
                .filter(models.SubmitOld.hid == hid) \
                .filter(models.SubmitOld.bid == bid) \
                .filter(models.SubmitOld.uid == uid) \
                .order_by(models.SubmitOld.submit_dt.desc(), models.SubmitOld.public_score.asc()) \
                .with_entities(
                    models.SubmitOld.id,
                    func.row_number().over().label('rank'),
                    models.SubmitOld.public_score,
                    models.SubmitOld.submit_dt,
                    models.SubmitOld.stared_flg,
                    models.SubmitOld.comment,
                    models.SubmitOld.file_location) \
                .all()
        else:
            user_submits = crud.get_submits(db=db, query=True) \
                .filter(models.SubmitOld.hid == hid) \
                .filter(models.SubmitOld.bid == bid) \
                .filter(models.SubmitOld.uid == uid) \
                .order_by(models.SubmitOld.submit_dt.desc(), models.SubmitOld.public_score.desc()) \
                .with_entities(
                    models.SubmitOld.id,
                    func.row_number().over().label('rank'),
                    models.SubmitOld.public_score,
                    models.SubmitOld.submit_dt,
                    models.SubmitOld.stared_flg,
                    models.SubmitOld.comment,
                    models.SubmitOld.file_location) \
                .all()

    return user_submits


def get_final_leaderboard(
    db: Session,
    hid: int,
    bid: int,
    uid: Optional[int] = None,
    org: Optional[str] = '',
):
    if uid is None:
        if hid == 3:
            if bid == 2:
                team_participant = crud.get_teams(db=db, query=True) \
                    .join(models.TeamParticipantsOld, models.TeamParticipantsOld.team_id == models.TeamOld.id, isouter=True) \
                    .filter(models.TeamOld.active == 1) \
                    .filter(or_(models.TeamParticipantsOld.status == 1, models.TeamParticipantsOld.status == None)) \
                    .with_entities(
                        models.TeamOld.name,
                        models.TeamOld.owner_id,
                        models.TeamParticipantsOld.user_id,
                    ) \
                    .subquery()

                teams_submits_1 = crud.get_submits(db=db, query=True) \
                    .join(team_participant, models.SubmitOld.uid == team_participant.c.owner_id) \
                    .filter(models.SubmitOld.hid == hid) \
                    .filter(models.SubmitOld.bid == bid) \
                    .with_entities(
                        team_participant.c.name,
                        models.SubmitOld.public_score,
                    )
                teams_submits_2 = crud.get_submits(db=db, query=True) \
                    .join(team_participant, models.SubmitOld.uid == team_participant.c.user_id) \
                    .filter(models.SubmitOld.hid == hid) \
                    .filter(models.SubmitOld.bid == bid) \
                    .with_entities(
                        team_participant.c.name,
                        models.SubmitOld.public_score,
                    )
                teams_submits = teams_submits_1.union(teams_submits_2).all()
                teams_submits_list = [[x.name, x.public_score] for x in teams_submits]
                result = {}
                for l in teams_submits_list:
                    if result.get(l[0], None) is None:
                        result[l[0]] = l[1]
                    elif result.get(l[0]) < l[1]:
                        result[l[0]] = l[1]
                    else:
                        pass
                return [dict(zip(['rank', 'name', 'public_score'], [x[0] + 1, x[1][0], x[1][1]])) for x in enumerate(sorted(result.items(), key=lambda k: -k[1]))]
            if bid == 1:
                team_participant = crud.get_teams(db=db, query=True) \
                    .join(models.TeamParticipantsOld, models.TeamParticipantsOld.team_id == models.TeamOld.id, isouter=True) \
                    .filter(models.TeamOld.active == 1) \
                    .filter(or_(models.TeamParticipantsOld.status == 1, models.TeamParticipantsOld.status == None)) \
                    .with_entities(
                        models.TeamOld.name,
                        models.TeamOld.owner_id,
                        models.TeamParticipantsOld.user_id,
                    ) \
                    .subquery()

                teams_submits_1 = crud.get_submits(db=db, query=True) \
                    .join(team_participant, models.SubmitOld.uid == team_participant.c.owner_id) \
                    .filter(models.SubmitOld.hid == hid) \
                    .filter(models.SubmitOld.bid == bid) \
                    .with_entities(
                        team_participant.c.name,
                        models.SubmitOld.public_score,
                        models.SubmitOld.public_score_1,
                    )
                teams_submits_2 = crud.get_submits(db=db, query=True) \
                    .join(team_participant, models.SubmitOld.uid == team_participant.c.user_id) \
                    .filter(models.SubmitOld.hid == hid) \
                    .filter(models.SubmitOld.bid == bid) \
                    .with_entities(
                        team_participant.c.name,
                        models.SubmitOld.public_score,
                        models.SubmitOld.public_score_1,
                    )
                teams_submits = teams_submits_1.union(teams_submits_2).all()
                teams_submits_list = [[x.name, x.public_score, x.public_score_1, x.public_score + x.public_score_1, str(round(x.public_score, 3)) + '/' + str(round(x.public_score_1, 3))] for x in teams_submits]
                result = {}
                for l in teams_submits_list:
                    if result.get(l[0], None) is None:
                        result[l[0]] = (l[3], l[4])
                    elif result.get(l[0])[0] < l[3]:
                        result[l[0]] = (l[3], l[4])
                    else:
                        pass
                return [dict(zip(['rank', 'name', 'public_score'], [x[0] + 1, x[1][0], x[1][1][1]])) for x in enumerate(sorted(result.items(), key=lambda k: -k[1][0]))]
        elif hid == 7:
            sort_rank = func.rank().over(partition_by=models.SubmitOld.uid,
                                         order_by=[models.SubmitOld.score.asc(),
                                                   models.SubmitOld.submit_dt.asc()]).label('sort_rank')
            pre_leaderboard = crud.get_submits(db=db, query=True) \
                .filter(models.SubmitOld.hid == hid) \
                .filter(models.SubmitOld.bid == bid) \
                .with_entities(
                    models.SubmitOld.uid,
                    models.SubmitOld.score,
                    sort_rank,
                ) \
                .subquery('t1')
            if len(org) > 0:
                leaderboard = crud.get_users(db=db, query=True) \
                    .join(pre_leaderboard, models.UserOld.id == pre_leaderboard.c.uid) \
                    .filter(pre_leaderboard.c.sort_rank == 1) \
                    .filter(models.UserOld.org == org) \
                    .order_by(pre_leaderboard.c.score.asc()) \
                    .with_entities(
                        func.row_number().over().label('rank'),
                        models.UserOld.name,
                        func.concat(func.round(pre_leaderboard.c.score, 4)).label('public_score'),
                    ) \
                    .all()
            else:
                leaderboard = crud.get_users(db=db, query=True) \
                    .join(pre_leaderboard, models.UserOld.id == pre_leaderboard.c.uid) \
                    .filter(pre_leaderboard.c.sort_rank == 1) \
                    .order_by(pre_leaderboard.c.score.asc()) \
                    .with_entities(
                        func.row_number().over().label('rank'),
                        models.UserOld.name,
                        func.concat(func.round(pre_leaderboard.c.score, 4)).label('public_score'),
                    ) \
                    .all()
            return leaderboard
        sort_rank = func.rank().over(partition_by=models.SubmitOld.uid, order_by=[models.SubmitOld.public_score.desc(), models.SubmitOld.submit_dt.asc()]).label('sort_rank')
        pre_leaderboard = crud.get_submits(db=db, query=True) \
            .filter(models.SubmitOld.hid == hid) \
            .filter(models.SubmitOld.bid == bid) \
            .with_entities(
                models.SubmitOld.uid,
                models.SubmitOld.public_score,
                sort_rank,
            ) \
            .subquery('t1')
        if len(org) > 0:
            leaderboard = crud.get_users(db=db, query=True) \
                .join(pre_leaderboard, models.UserOld.id == pre_leaderboard.c.uid) \
                .filter(pre_leaderboard.c.sort_rank == 1) \
                .filter(models.UserOld.org == org) \
                .order_by(pre_leaderboard.c.public_score.desc()) \
                .with_entities(
                    func.row_number().over().label('rank'),
                    models.UserOld.name,
                    func.concat(func.round(pre_leaderboard.c.public_score, 3)).label('public_score'),
                ) \
                .all()
        else:
            leaderboard = crud.get_users(db=db, query=True) \
                .join(pre_leaderboard, models.UserOld.id == pre_leaderboard.c.uid) \
                .filter(pre_leaderboard.c.sort_rank == 1) \
                .order_by(pre_leaderboard.c.public_score.desc()) \
                .with_entities(
                func.row_number().over().label('rank'),
                models.UserOld.name,
                func.concat(func.round(pre_leaderboard.c.public_score, 3)).label('public_score'),
            ) \
                .all()
        return leaderboard
    else:
        if hid == 3:
            if bid == 1 or bid == 2:
                team_participant = crud.get_teams(db=db, query=True) \
                    .join(models.TeamParticipantsOld, models.TeamParticipantsOld.team_id == models.TeamOld.id, isouter=True) \
                    .filter(models.TeamOld.active == 1) \
                    .filter(or_(models.TeamParticipantsOld.status == 1, models.TeamParticipantsOld.status == None)) \
                    .filter(or_(models.TeamParticipantsOld.user_id == uid, models.TeamOld.owner_id == uid)) \
                    .with_entities(
                        models.TeamOld.owner_id,
                        models.TeamParticipantsOld.user_id,
                    ) \
                    .all()
                if len(team_participant) == 0:
                    user_submits = crud.get_submits(db=db, query=True) \
                        .filter(models.SubmitOld.hid == hid) \
                        .filter(models.SubmitOld.bid == bid) \
                        .filter(models.SubmitOld.uid == uid) \
                        .order_by(models.SubmitOld.submit_dt.desc(), models.SubmitOld.public_score.desc()) \
                        .with_entities(
                            models.SubmitOld.id,
                            func.row_number().over().label('rank'),
                            models.SubmitOld.public_score,
                            models.SubmitOld.submit_dt,
                            models.SubmitOld.stared_flg,
                            models.SubmitOld.comment,
                            models.SubmitOld.file_location) \
                        .all()
                else:
                    user_submits = crud.get_submits(db=db, query=True) \
                        .filter(models.SubmitOld.hid == hid) \
                        .filter(models.SubmitOld.bid == bid) \
                        .filter(or_(models.SubmitOld.uid == team_participant[0].owner_id, models.SubmitOld.uid == team_participant[0].user_id)) \
                        .order_by(models.SubmitOld.submit_dt.desc(), models.SubmitOld.public_score.desc()) \
                        .with_entities(
                            models.SubmitOld.id,
                            func.row_number().over().label('rank'),
                            models.SubmitOld.public_score,
                            models.SubmitOld.submit_dt,
                            models.SubmitOld.stared_flg,
                            models.SubmitOld.comment,
                            models.SubmitOld.file_location
                        ) \
                        .all()
            else:
                user_submits = crud.get_submits(db=db, query=True) \
                    .filter(models.SubmitOld.hid == hid) \
                    .filter(models.SubmitOld.bid == bid) \
                    .filter(models.SubmitOld.uid == uid) \
                    .order_by(models.SubmitOld.submit_dt.desc(), models.SubmitOld.public_score.desc()) \
                    .with_entities(
                    models.SubmitOld.id,
                    func.row_number().over().label('rank'),
                    models.SubmitOld.public_score,
                    models.SubmitOld.submit_dt,
                    models.SubmitOld.stared_flg,
                    models.SubmitOld.comment,
                    models.SubmitOld.file_location) \
                    .all()
        elif hid == 7:
            user_submits = crud.get_submits(db=db, query=True) \
                .filter(models.SubmitOld.hid == hid) \
                .filter(models.SubmitOld.bid == bid) \
                .filter(models.SubmitOld.uid == uid) \
                .order_by(models.SubmitOld.submit_dt.desc(), models.SubmitOld.public_score.asc()) \
                .with_entities(
                    models.SubmitOld.id,
                    func.row_number().over().label('rank'),
                    models.SubmitOld.public_score,
                    models.SubmitOld.submit_dt,
                    models.SubmitOld.stared_flg,
                    models.SubmitOld.comment,
                    models.SubmitOld.file_location) \
                .all()
        else:
            user_submits = crud.get_submits(db=db, query=True) \
                .filter(models.SubmitOld.hid == hid) \
                .filter(models.SubmitOld.bid == bid) \
                .filter(models.SubmitOld.uid == uid) \
                .order_by(models.SubmitOld.submit_dt.desc(), models.SubmitOld.public_score.desc()) \
                .with_entities(
                    models.SubmitOld.id,
                    func.row_number().over().label('rank'),
                    models.SubmitOld.public_score,
                    models.SubmitOld.submit_dt,
                    models.SubmitOld.stared_flg,
                    models.SubmitOld.comment,
                    models.SubmitOld.file_location) \
                .all()

    return user_submits


def get_best_score(
    db: Session,
    hid: int,
    bid: int,
    uid: int,
):
    user_submits = crud.get_submits(db=db, query=True) \
        .filter(models.SubmitOld.hid == hid) \
        .filter(models.SubmitOld.bid == bid) \
        .filter(models.SubmitOld.uid == uid) \
        .with_entities(
            func.max(models.SubmitOld.public_score).label('max_score'),
            func.count(models.SubmitOld.public_score).label('submits_num'),
        ) \
        .first()

    return user_submits


def get_day_submits_num(
    db: Session,
    hid: int,
    bid: int,
    uid: int,
):
    user_submits_day_count = crud.get_submits(db=db, query=True) \
        .filter(models.SubmitOld.hid == hid) \
        .filter(models.SubmitOld.bid == bid) \
        .filter(models.SubmitOld.uid == uid) \
        .filter(func.day(models.SubmitOld.submit_dt) == datetime.datetime.now(pytz.timezone('Europe/Moscow')).day) \
        .filter(func.month(models.SubmitOld.submit_dt) == datetime.datetime.now(pytz.timezone('Europe/Moscow')).month) \
        .filter(func.year(models.SubmitOld.submit_dt) == datetime.datetime.now(pytz.timezone('Europe/Moscow')).year) \
        .count()

    return user_submits_day_count


def star_submit_star_flag(db: Session, sid: int):
    edited_submit = crud.get_submits(db=db, query=True) \
        .filter(models.SubmitOld.id == sid) \
        .options(
            load_only('public_score', 'stared_flg', 'comment', 'file_location',)) \
        .first()
    setattr(edited_submit, 'stared_flg', True)
    # edited_submit.stared_flg = True
    db.commit()
    db.refresh(edited_submit)

    return edited_submit


def unstar_submit_star_flag(db: Session, sid: int):
    edited_submit = crud.get_submits(db=db, query=True) \
        .filter(models.SubmitOld.id == sid) \
        .options(
            load_only('public_score', 'stared_flg', 'comment', 'file_location',)) \
        .first()
    setattr(edited_submit, 'stared_flg', False)
    # edited_submit.stared_flg = False
    db.commit()
    db.refresh(edited_submit)

    return edited_submit


def count_submit_star_flags(db: Session, uid: int, hid: int):
    return crud.get_submits(db=db, query=True) \
        .filter(models.SubmitOld.uid == uid) \
        .filter(models.SubmitOld.hid == hid) \
        .with_entities(func.sum(models.SubmitOld.stared_flg.cast(Integer)).label('flags_num')) \
        .first().flags_num


def update_score(db, id, score):
    user = crud.get_submits(db=db, query=True).get(id)
    setattr(user, 'public_score', score)
    db.commit()
    db.flush()



def get_final_leaderboard_mkb(
    db: Session,
    hid: int,
    bid: int,
    org: Optional[str] = '',
):
    sort_rank1 = func.rank()\
        .over(
            partition_by=models.SubmitOld.uid,
            order_by=[
                models.SubmitOld.stared_flg.desc(),
                models.SubmitOld.public_score.desc(),
                models.SubmitOld.submit_dt.asc()])\
        .label('sort_rank1')
    pre_leaderboard = crud.get_submits(db=db, query=True) \
        .filter(models.SubmitOld.hid == hid) \
        .filter(models.SubmitOld.bid == bid) \
        .with_entities(
            models.SubmitOld.uid,
            models.SubmitOld.public_score,
            models.SubmitOld.private_score,
            models.SubmitOld.submit_dt,
            sort_rank1,
        ).subquery('t1')
    sort_rank2 = func.rank() \
        .over(
        partition_by=pre_leaderboard.c.uid,
        order_by=[
            pre_leaderboard.c.private_score.desc(),
            pre_leaderboard.c.submit_dt.asc()]) \
        .label('sort_rank2')
    top2_submits = db.query(pre_leaderboard) \
        .filter(pre_leaderboard.c.sort_rank1 < 3) \
        .with_entities(
            pre_leaderboard.c.uid,
            pre_leaderboard.c.public_score,
            pre_leaderboard.c.private_score,
            pre_leaderboard.c.submit_dt,
            sort_rank2,
        ).subquery('t2')

    leaderboard = crud.get_users(db=db, query=True) \
        .filter(top2_submits.c.sort_rank2 == 1) \
        .join(top2_submits, models.UserOld.id == top2_submits.c.uid) \
        .order_by(top2_submits.c.private_score.desc()) \
        .with_entities(
            func.row_number().over().label('rank'),
            models.UserOld.name,
            models.UserOld.email,
            models.UserOld.firstname,
            models.UserOld.lastname,
            models.UserOld.city,
            func.concat(func.round(top2_submits.c.public_score, 4)).label('public_score'),
            func.concat(func.round(top2_submits.c.private_score, 4)).label('private_score'),
            top2_submits.c.submit_dt,
        ) \
        .all()

    return leaderboard


def get_submits_num(
    db: Session,
    hid: int,
    bid: int,
):
    submits_by_days = crud.get_submits(db=db, query=True) \
        .filter(models.SubmitOld.hid == hid) \
        .filter(models.SubmitOld.bid == bid) \
        .with_entities(
            func.date_format(models.SubmitOld.submit_dt, '%Y-%m-%d').label('dt'),
            models.SubmitOld.id,
        ).subquery('t1')

    submits_num = db.query(submits_by_days) \
        .with_entities(
            submits_by_days.c.dt,
            func.count(submits_by_days.c.id).label('count'),
        ) \
        .group_by(submits_by_days.c.dt) \
        .subquery()
    return submits_num


def get_active_users_num(
    db: Session,
    hid: int,
    bid: int,
):
    users_by_days = crud.get_submits(db=db, query=True) \
        .filter(models.SubmitOld.hid == hid) \
        .filter(models.SubmitOld.bid == bid) \
        .with_entities(
            func.date_format(models.SubmitOld.submit_dt, '%Y-%m-%d').label('dt'),
            models.SubmitOld.uid,
        ).subquery('t1')

    active_users_num = db.query(users_by_days) \
        .with_entities(
            users_by_days.c.dt,
            func.group_concat(distinct(users_by_days.c.uid)).label('count'),
        ) \
        .group_by(users_by_days.c.dt) \
        .subquery()
    return active_users_num


def get_users_num(
    db: Session,
    hid: int,
    bid: int,
    org: Optional[str] = '',
):
    # active_users = crud.get_submits(db=db, query=True) \
    #     .filter(models.SubmitOld.hid == hid) \
    #     .filter(models.SubmitOld.bid == bid) \
    #     .with_entities(
    #         models.SubmitOld.uid.label('id'),
    #     )

    # org_users = crud.get_users(db=db, query=True) \
    #     .filter(models.UserOld.org == org) \
    #     .with_entities(
    #         models.UserOld.id,
    #     )

    add_users = crud.get_users(db=db, query=True) \
        .filter(((models.UserOld.reg_dt >= '2021-11-01') & (models.UserOld.reg_dt < '2021-11-03')) | (models.UserOld.org == org)) \
        .with_entities(
            models.UserOld.id,
            func.date_format(models.UserOld.reg_dt, '%Y-%m-%d').label('dt'),
        ).subquery('t1')

    # all_ids = union(org_users, add_users).subquery('t1')
    # all_ids = add_users.subquery()
    # all_users = crud.get_users(db=db, query=True) \
    #     .join(all_ids, models.UserOld.id == all_ids.c.id) \
    #     .with_entities(
    #         models.UserOld.id,
    #         func.date_format(models.UserOld.reg_dt, '%Y-%m-%d').label('dt'),
    #     ).subquery('t2')

    all_users_num = db.query(add_users) \
        .with_entities(
            add_users.c.dt,
            func.count(distinct(add_users.c.id)).label('count'),
        ) \
        .group_by(add_users.c.dt) \
        .subquery('t2')
    return all_users_num


def get_participants_stats(
    db: Session,
    hid: int,
    bid: int,
    org: Optional[str] = '',
):
    active_users_num = get_active_users_num(db, hid, bid)
    submits_num = get_submits_num(db, hid, bid)
    users_num = get_users_num(db, hid, bid, org)

    all_stats = db.query(users_num)\
        .join(active_users_num, users_num.c.dt == active_users_num.c.dt) \
        .join(submits_num, users_num.c.dt == submits_num.c.dt) \
        .with_entities(
            users_num.c.dt,
            users_num.c.count.label('users_num'),
            active_users_num.c.count.label('active_users_num'),
            submits_num.c.count.label('submits_num'),
        ) \
        .order_by(users_num.c.dt.asc()).all()
    a = np.array(all_stats)
    print(a)
    f = lambda x: dict(zip(['dt', 'users_num', 'active_users_num', 'submits_num'], x))
    a[:, 3] = np.cumsum(list(map(int, a[:, 3])))
    au = []
    for i, value in enumerate(a[:, 2]):
        tmp = [x.split(',') for x in a[:i + 1, 2]]
        au.append(len(set([item for sublist in tmp for item in sublist])))
    a[:, 2] = au
    a[:, 1] = np.cumsum(list(map(int, a[:, 1]))) + (607 - 581)
    print(a)
    return [f(x) for x in a.tolist()]