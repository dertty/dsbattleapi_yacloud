import datetime
import secrets
import csv
from io import StringIO
from fastapi.responses import StreamingResponse
from typing import List, Optional
from sqlalchemy import func, or_

import numpy as np
import pytz
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, UploadFile, File
from fastapi.responses import StreamingResponse
from jinja2 import utils
from sklearn.metrics import accuracy_score, mean_absolute_error, roc_auc_score
from sqlalchemy.orm import Session, aliased

from api.api_v1.core.credentials import API_ACCESS_TOKEN
from api.api_v1.core.database import get_db
from api.api_v1.core.functions.s3 import get_file_from_s3, save_file_to_s3, get_file_from_s3_bytes
from api.api_v1.core.models import schemas, models
from api.api_v1.core.models.crud import crud, submits, users, teams
from api_versioning import versioned_api_route

import pandas as pd

router = APIRouter(route_class=versioned_api_route(1, 0))


def check_access_token(token, real_token=API_ACCESS_TOKEN):
    correct_token = secrets.compare_digest(token, real_token)
    if not correct_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect login",
        )


@router.post(
    "/submit_file",
    summary="Сохранение результатов сабмита",
    tags=["submit"])
def create_submit(
        uid: int,
        hid: int,
        bid: int,
        background_tasks: BackgroundTasks,
        comment: Optional[str] = None,
        file: UploadFile = File(...),
        db: Session = Depends(get_db),
        access_token: str = '',
):
    check_access_token(access_token)
    comment = str(utils.escape(comment))
    new_file_name = file.filename.split(".")[0] + \
                    "__" + \
                    datetime.datetime.now(pytz.timezone('Europe/Moscow')).isoformat() + \
                    "." + \
                    file.filename.split(".")[1]
    file_location = f'hackathons_submits/hid{hid}/uid{uid}/bid{bid}/{new_file_name}'

    if hid == 5: # junior ds
        if submits.get_day_submits_num(db=db, hid=hid, bid=bid, uid=uid) < 3:
            try:
                uploaded_file = np.genfromtxt(StringIO(file.file.read().decode('utf8')), delimiter=';', skip_header=True)
                file_for_public_score = f'hackathons_resources/hid{hid}/public_dataset.csv'
                file_for_public_score = np.genfromtxt(
                    StringIO(get_file_from_s3(file_for_public_score).decode('utf8')),
                    delimiter=';', skip_header=True)
                public_score = float(accuracy_score(file_for_public_score[:, 1], uploaded_file[:, 1]))
                submit = schemas.SubmitCreate(hid=hid, bid=bid, uid=uid, public_score=public_score, comment=comment,
                                              file_location=file_location, )

                def jobs_for_background():
                    save_file_to_s3(file.file, file_location)
                    file_for_private_score = f'hackathons_resources/hid{hid}/private_dataset.csv'
                    file_for_private_score = np.genfromtxt(
                        StringIO(get_file_from_s3(file_for_private_score).decode('utf-8')),
                        delimiter=';', skip_header=True)
                    private_score = float(accuracy_score(file_for_private_score[:, 1], uploaded_file[:, 1]))
                    submit.private_score = private_score
                    crud.create_submit(db=db, submit=submit)

                background_tasks.add_task(jobs_for_background)
            except:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Error while loading submit, check file.",
                )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Exceeded the submits limit",
            )
    elif hid == 9: # junior ds
        if submits.get_day_submits_num(db=db, hid=hid, bid=bid, uid=uid) < 5:
            try:
                uploaded_file = np.genfromtxt(StringIO(file.file.read().decode('utf8')), delimiter=';', skip_header=True)
                file_for_public_score = f'hackathons_resources/hid{hid}/public_dataset.csv'
                file_for_public_score = np.genfromtxt(
                    StringIO(get_file_from_s3(file_for_public_score).decode('utf8')),
                    delimiter=';', skip_header=True)
                public_score = float(accuracy_score(file_for_public_score[:, 1], uploaded_file[:, 1]))
                submit = schemas.SubmitCreate(hid=hid, bid=bid, uid=uid, public_score=public_score, comment=comment,
                                              file_location=file_location, )

                def jobs_for_background():
                    save_file_to_s3(file.file, file_location)
                    file_for_private_score = f'hackathons_resources/hid{hid}/private_dataset.csv'
                    file_for_private_score = np.genfromtxt(
                        StringIO(get_file_from_s3(file_for_private_score).decode('utf-8')),
                        delimiter=';', skip_header=True)
                    private_score = float(accuracy_score(file_for_private_score[:, 1], uploaded_file[:, 1]))
                    submit.private_score = private_score
                    crud.create_submit(db=db, submit=submit)

                background_tasks.add_task(jobs_for_background)
            except:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Error while loading submit, check file.",
                )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Exceeded the submits limit",
            )
    elif hid == 8: # hackathom mkb
        if bid == 0:
            utc = pytz.UTC
            if submits.get_day_submits_num(db=db, hid=hid, bid=bid, uid=uid) < 5:
                    try:
                        uploaded_file = np.genfromtxt(StringIO(file.file.read().decode('utf8')), delimiter=';', skip_header=1)
                        if len(uploaded_file[:, 1]) == 7330:
                            file_for_public_score = f'hackathons_resources/hid{hid}/bid{bid}/public_dataset.csv'
                            file_for_public_score = np.genfromtxt(
                                StringIO(get_file_from_s3(file_for_public_score).decode('utf8')),
                                delimiter=';', skip_header=1)
                            file_for_public_score = dict(file_for_public_score)
                            uploaded_file = dict(uploaded_file)
                            y_true = []
                            y_pred = []
                            for k in file_for_public_score:
                                y_true.append(file_for_public_score[k])
                                y_pred.append(uploaded_file[k])

                            public_score = float(roc_auc_score(y_true, y_pred))
                        else:
                            return "Проверьте файл, неправильное количество наблюдений."
                    except:
                        return "Проверьте файл, ошибка при чтении файла."

                    submit = schemas.SubmitCreate(
                        hid=hid, bid=bid, uid=uid,
                        public_score=public_score,
                        comment=comment, file_location=file_location, )

                    def jobs_for_background():
                        save_file_to_s3(file.file, file_location)
                        file_for_private_score = f'hackathons_resources/hid{hid}/bid{bid}/private_dataset.csv'
                        file_for_private_score = np.genfromtxt(
                            StringIO(get_file_from_s3(file_for_private_score).decode('utf-8')),
                            delimiter=';', skip_header=1)

                        file_for_private_score = dict(file_for_private_score)
                        y_true = []
                        y_pred = []
                        for k in file_for_private_score:
                            y_true.append(file_for_private_score[k])
                            y_pred.append(uploaded_file[k])

                        private_score = float(roc_auc_score(y_true, y_pred))

                        submit.private_score = private_score
                        crud.create_submit(db=db, submit=submit)

                    background_tasks.add_task(jobs_for_background)
                    return "Отправка решений для доступна с 00:00 1 ноября по 23:59 30 ноября по Московскому времени."
            else:
                return "Превышено количество сабмитов. " +\
                           "В день доступно 5 сабмитов. " +\
                           "Количество сабмитов сбрасывается в 00:00 (Московское время)."
    elif hid == 10: # crypto
        if bid == 0:
            utc = pytz.UTC
            if submits.get_day_submits_num(db=db, hid=hid, bid=bid, uid=uid) < 5:
                # try:
                uploaded_file = pd.read_csv(StringIO(file.file.read().decode('utf8')), sep=',',)
                if uploaded_file.shape[0] > 2:

                    def get_revenue(df: pd.DataFrame, deals_column='pred', price_column='price',) -> float:
                        '''
                        1 - sell
                        2 - buy
                        Does trading always start with buying money?
                        '''
                        df = df[[deals_column, price_column]].reset_index(drop=True)[[deals_column, price_column]].copy()

                        first_buy_index = df[df[deals_column] == 2].first_valid_index()
                        df = df.iloc[first_buy_index:]
                        df = df[df[deals_column] > 0]
                        # first buy and first sell after buy
                        # left only buy-sell pairs and ignore all transactions between them (if that transactions don't form a pair)
                        df = df[df[deals_column] != df[deals_column].shift(1)]
                        return (- df[price_column][::2] + df[price_column].shift(-1)[::2]).fillna(0).sum()

                    public_score = get_revenue(uploaded_file)
                else:
                    return "Проверьте файл, неправильное количество наблюдений."
                # except:
                #     return "Проверьте файл, ошибка при чтении файла."

                submit = schemas.SubmitCreate(
                    hid=hid, bid=bid, uid=uid,
                    public_score=public_score,
                    comment=comment, file_location=file_location, )
                return submit
                return "Отправка решений для доступна с 00:00 1 ноября по 23:59 30 ноября по Московскому времени."
            else:
                return "Превышено количество сабмитов. " + \
                       "В день доступно 5 сабмитов. " + \
                       "Количество сабмитов сбрасывается в 00:00 (Московское время)."
    else:
        return "Приём решений закрыт."
    return submit


@router.get(
    "/leaderboard",
    response_model=List[schemas.LeaderBoard],
    summary="Получение лидерборда",
    tags=["submits"], )
def get_leaderboard(hid: int, bid: int, db: Session = Depends(get_db), access_token: Optional[str] = '', org: Optional[str] = ''):
    check_access_token(access_token)
    return submits.get_leaderboard(db=db, hid=hid, bid=bid, org=org)


@router.get(
    "/user_submits",
    response_model=List[schemas.UserSubmits],
    summary="Получение сабмитов пользователя",
    tags=["submits"])
def get_user_submits(hid: int, bid: int, uid: int, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    return submits.get_leaderboard(db=db, hid=hid, bid=bid, uid=uid)


@router.get(
    "/max_score",
    response_model=schemas.MaxScore,
    summary="Получение максимального результата у пользователя",
    tags=["submits"], )
def max_score(hid: int, bid: int, uid: int, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    return submits.get_best_score(db=db, hid=hid, bid=bid, uid=uid)


@router.post(
    "/login_nti_user",
    # response_model=schemas.User,
    summary="Аунтификация пользователя nti",
    tags=["login"], )
def login_nti_user(user_details: schemas.TalentUser, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    return users.login_nti_user(db=db, user_details=user_details)


@router.get(
    "/get_time",
    summary="Время сервера",
    tags=["others"], )
def get_time():
    return datetime.datetime.today()


@router.get(
    "/get_submit_file",
    summary="Получение решений, отправленных участниками",
    tags=["submits"], )
def get_submit_file(
        sid: int,
        uid: int,
        db: Session = Depends(get_db),
        access_token: Optional[str] = ''
):
    check_access_token(access_token)
    sid = crud.get_submits(db, query=True).filter(models.SubmitOld.id == sid).first()
    if sid.uid == uid:
        file_like = get_file_from_s3_bytes(sid.file_location)
        return StreamingResponse(file_like, media_type="text/plain")
    else:
        raise HTTPException(status_code=400, detail="Not allowed, wrong file.")


@router.put(
    "/user",
    summary="Изменение имени пользователя",
    tags=["user"], )
def change_username(user_details: schemas.UserInfo, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    user_details.name = str(utils.escape(user_details.name))
    if user_details.user_sender_id == user_details.id:
        if len(user_details.name) < 3:
            return {'success': 'Длина никнейма должна быть больше 2 символов.'}
        if len(crud.get_users(db=db, query=True).filter(models.UserOld.name == user_details.name).all()) > 0:
            return {'success': 'Такой никнейм уже занят.'}
        else:
            users.change_name(db, user_details.id, user_details.name)
            return {'success': 1}
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect login", )


@router.get(
    "/star_submit",
    summary="Отметить сабмит как основной",
    tags=["submits"])
def star_submit(sid: int, uid: int, action: int, hid: int, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    utc = pytz.UTC
    if utc.localize(datetime.datetime(2021, 11, 1, 0, 0, 0)) <= datetime.datetime.now(pytz.timezone('Europe/Moscow')) < utc.localize(datetime.datetime(2021, 11, 30, 23, 59, 0)):
        if action == 0:
            flags_num = submits.count_submit_star_flags(db=db, uid=uid, hid=hid)
            if flags_num is not None:
                if flags_num < 2:
                    return submits.star_submit_star_flag(db=db, sid=sid)
                else:
                    raise HTTPException(status_code=400, detail="Allowed only two stared submits")
            else:
                raise HTTPException(status_code=400, detail="Invalid values")
        else:
            return submits.unstar_submit_star_flag(db=db, sid=sid)
    else:
        raise HTTPException(status_code=400, detail="Хакатон завершился")


@router.put(
    "/create_team",
    # response_model=schemas.TeamCreate,
    summary="Создать команду",
    tags=["teams"])
def create_team(team_details: schemas.TeamCreate, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)

    team_details.name = str(utils.escape(team_details.name))
    if len(team_details.name) < 3:
        return {'success': 'Длина никнейма должна быть больше 2 символов.'}
    if len(crud.get_teams(db=db, query=True).filter(models.TeamOld.active == 1).filter(
            models.TeamOld.name == team_details.name).all()) > 0:
        return {'success': 'Такой никнейм уже занят.'}
    if len(teams.get_team(db, team_details.owner_id)['owner']) > 0:
        return {'success': 'У вас уже есть команда.'}
    else:
        crud.get_participants(db=db, query=True).filter(
            models.TeamParticipantsOld.user_id == team_details.owner_id).update({'status': 2})
        db.commit()
        crud.create_team(db, team_details)
        return {'success': 1}


@router.put(
    "/leave_team",
    summary="Получение команд участника",
    tags=["teams"], )
def delete_invite(details: schemas.TeamInviteDelete, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    if details.team_id is not None:
        invite = crud.get_participants(db=db, query=True).get(details.team_id)
    else:
        invite = crud.get_participants(db=db, query=True).get(details.invite_id)
    setattr(invite, 'status', 2)
    db.commit()
    db.flush()
    return {'success': 1}


@router.put(
    "/invite_to_team",
    summary="Получение команд участника",
    tags=["teams"], )
def invite_to_team(team_details: schemas.TeamInvite, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    from sqlalchemy import func
    user_id = crud.get_users(db=db, query=True) \
        .filter(func.lower(models.UserOld.email) == func.lower(team_details.participant_email)) \
        .first()

    if user_id is None:
        return {'success': 'Такой пользователь не найден.'}
    if len(crud.get_participants(db, query=True).filter(models.TeamParticipantsOld.user_id == user_id.id).filter(
            models.TeamParticipantsOld.status == 1).all()) > 0:
        return {'success': 'У пользователя уже есть команда.'}
    if len(crud.get_teams(db, query=True).filter(models.TeamOld.active == 1).filter(
            models.TeamOld.owner_id == user_id.id).all()) > 0:
        return {'success': 'У пользователя уже есть команда.'}
    crud.create_team_participant(db, team_id=team_details.team_id, user_id=user_id.id, status=0)
    crud.get_participants(db=db, query=True).filter(models.TeamParticipantsOld.user_id == team_details.owner_id).update(
        {'status': 2})
    db.commit()
    return {'success': 1}


@router.put(
    "/approve_team_invite",
    summary="Получение команд участника",
    tags=["teams"], )
def approve_team_invite(team_details: schemas.TeamInviteApprove, db: Session = Depends(get_db),
                        access_token: Optional[str] = ''):
    check_access_token(access_token)
    user = crud.get_participants(db=db, query=True).get(team_details.id)
    setattr(user, 'status', 1)
    db.commit()
    db.flush()
    crud.get_participants(db=db, query=True).filter(models.TeamParticipantsOld.user_id == team_details.user_id).filter(
        models.TeamParticipantsOld.id != team_details.id).update({'status': 2})
    db.commit()
    return {'success': 1}


@router.put(
    "/delete_invite",
    summary="Получение команд участника",
    tags=["teams"], )
def delete_invite(details: schemas.TeamInviteDelete, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    invite = crud.get_participants(db=db, query=True).get(details.invite_id)
    setattr(invite, 'status', 2)
    db.commit()
    db.flush()
    return {'success': 1}


@router.put(
    "/delete_team",
    summary="Получение команд участника",
    tags=["teams"], )
def delete_invite(details: schemas.TeamDelete, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    team = crud.get_teams(db=db, query=True).get(details.team_id)
    setattr(team, 'active', 0)
    db.commit()
    db.flush()
    crud.get_participants(db=db, query=True).filter(models.TeamParticipantsOld.team_id == details.team_id).update(
        {'status': 2})
    db.commit()
    return {'success': 1}


@router.get(
    "/get_team",
    summary="Получение команд участника",
    tags=["teams"], )
def get_team(uid: int, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    return teams.get_team(db, uid)


@router.get(
    "/get_team",
    summary="Получение команд участника",
    tags=["teams"], )
def get_team(uid: int, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    return teams.get_team(db, uid)


@router.get(
    "/get_submits_info",
    summary="Получение команд участника",
    tags=["dashboard"], )
def get_team(hid: int, bid: int, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    # check_access_token(access_token)
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
            models.SubmitOld.private_score,
            models.SubmitOld.score,
        )
    teams_submits_2 = crud.get_submits(db=db, query=True) \
        .join(team_participant, models.SubmitOld.uid == team_participant.c.user_id) \
        .filter(models.SubmitOld.hid == hid) \
        .filter(models.SubmitOld.bid == bid) \
        .with_entities(
            team_participant.c.name,
            models.SubmitOld.public_score,
            models.SubmitOld.private_score,
            models.SubmitOld.score,
        )
    teams_submits = teams_submits_1.union(teams_submits_2).all()
    teams_submits_list = [[x.name, x.public_score, x.private_score, x.score] for x in teams_submits]

    stream = StringIO()
    writer = csv.writer(stream, dialect='excel')
    writer.writerows([['Название команды', 'public_score', 'private_score', 'score']] + list(teams_submits))

    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=submits_info_utf8.csv"
    return response


@router.get(
    "/get_teams_info",
    summary="Получение команд участника",
    tags=["dashboard"], )
def get_team(hid: int, bid: int, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    # check_access_token(access_token)
    owner = aliased(models.UserOld)
    participant = aliased(models.UserOld)

    team_participant = crud.get_teams(db=db, query=True) \
        .join(owner, owner.id == models.TeamOld.owner_id, isouter=True) \
        .join(models.TeamParticipantsOld, models.TeamParticipantsOld.team_id == models.TeamOld.id, isouter=True) \
        .join(participant, participant.id == models.TeamParticipantsOld.user_id, isouter=True) \
        .filter(models.TeamOld.active == 1) \
        .filter(or_(models.TeamParticipantsOld.status == 1, models.TeamParticipantsOld.status == None)) \
        .with_entities(
            models.TeamOld.name.label('Название команды'),
            owner.name.label('Создатель команды'),
            owner.email.label('Email создателя команды'),
            participant.name.label('Приглашённый в команду участник'),
            participant.email.label('Email пришлашённого участника'),
        ).all()
    print(team_participant)

    stream = StringIO()
    writer = csv.writer(stream, dialect='excel')
    writer.writerows([['Название команды', 'Создатель команды', 'Email создателя команды', 'Приглашённый в команду участник', 'Email пришлашённого участника',]] + list(team_participant))

    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=team_info_utf8.csv"
    return response


@router.get(
    "/get_final_leaderboard0",
    summary="Получение команд участника",
    tags=["dashboard"], )
def get_team(hid: int, bid: int, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    # check_access_token(access_token)
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
            models.SubmitOld.score,
        )
    teams_submits_2 = crud.get_submits(db=db, query=True) \
        .join(team_participant, models.SubmitOld.uid == team_participant.c.user_id) \
        .filter(models.SubmitOld.hid == hid) \
        .filter(models.SubmitOld.bid == bid) \
        .with_entities(
            team_participant.c.name,
            models.SubmitOld.public_score,
            models.SubmitOld.score,
        )
    teams_submits = list(teams_submits_1.union(teams_submits_2).all())
    result = {}
    for team in teams_submits:
        if result.get(team[0], None) is None:
            result[team[0]] = [team[1], team[2]]
        elif result[team[0]][0] < team[1]:
            result[team[0]] = [team[1], team[2]]
        else:
            pass

    stream = StringIO()
    writer = csv.writer(stream, dialect='excel')
    writer.writerows([['#', 'Название команды', 'score']] + [[x[0], x[1][0], x[1][1][1]] for x in enumerate(sorted(result.items(), key=lambda x: -x[1][1]))])

    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=final_leaderboard.csv"
    return response


@router.get(
    "/get_final_leaderboard2",
    summary="Получение команд участника",
    tags=["dashboard"], )
def get_team(hid: int, bid: int, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    # check_access_token(access_token)
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
            models.SubmitOld.score,
        )
    teams_submits_2 = crud.get_submits(db=db, query=True) \
        .join(team_participant, models.SubmitOld.uid == team_participant.c.user_id) \
        .filter(models.SubmitOld.hid == hid) \
        .filter(models.SubmitOld.bid == bid) \
        .with_entities(
            team_participant.c.name,
            models.SubmitOld.score,
        )
    teams_submits = teams_submits_1.union(teams_submits_2).subquery()

    teams_submits = list(db.query(teams_submits.c.name, func.max(teams_submits.c.score), func.min(teams_submits.c.score)).group_by(teams_submits.c.name).all())
    # teams_submits = list(teams_submits.group_by(teams_submits.c.name).with_entities(teams_submits.c.name, func.max(models.SubmitOld.score), func.min(models.SubmitOld.score)).all())
    result = {}
    for team in teams_submits:
        if result.get(team[0], None) is None:
            result[team[0]] = team[1]
        elif result[team[0]] < team[1]:
            result[team[0]] = team[1]
        else:
            pass

    stream = StringIO()
    writer = csv.writer(stream, dialect='excel')
    writer.writerows([['#', 'Название команды', 'score']] + [[x[0], x[1][0], x[1][1]] for x in enumerate(sorted(result.items(), key=lambda x: -x[1]))])

    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=final_leaderboard_minmax.csv"
    return response


@router.get(
    "/get_final_leaderboard3",
    summary="Получение команд участника",
    tags=["dashboard"], )
def get_team(hid: int, bid: int, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    # check_access_token(access_token)
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
    leaderboard = crud.get_users(db=db, query=True) \
        .join(pre_leaderboard, models.UserOld.id == pre_leaderboard.c.uid) \
        .filter(pre_leaderboard.c.sort_rank == 1) \
        .order_by(pre_leaderboard.c.score.asc()) \
        .with_entities(
            func.row_number().over().label('rank'),
            models.UserOld.name,
            func.round(pre_leaderboard.c.score, 4).label('score'),
        ) \
        .all()

    stream = StringIO()
    writer = csv.writer(stream, dialect='excel')
    writer.writerows([['rank', 'username', 'score']] + [[x[0], x[1], x[2]] for x in list(leaderboard)])

    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=final_leaderboard.csv"
    return response


@router.get(
    "/final_leaderboard",
    response_model=List[schemas.FinalLeaderBoard],
    summary="Получение лидерборда",
    tags=["submits"], )
def get_final_leaderboard(hid: int, bid: int, db: Session = Depends(get_db), access_token: Optional[str] = '', org: Optional[str] = ''):
    check_access_token(access_token)
    return submits.get_final_leaderboard_mkb(db=db, hid=hid, bid=bid, org=org)


@router.get(
    "/participants_stats",
    response_model=List[schemas.HackathonStats],
    summary="Количество участников, зарегистрированных на хакатоне",
    tags=["submits", "users", 'hackathon'], )
def get_participants_stats(hid: int, bid: int, db: Session = Depends(get_db), access_token: Optional[str] = '', org: Optional[str] = ''):
    check_access_token(access_token)
    return submits.get_participants_stats(db=db, hid=hid, bid=bid, org=org)