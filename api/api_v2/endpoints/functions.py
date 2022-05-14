import datetime
import secrets
from io import StringIO
from typing import List, Optional

import numpy as np
import pytz
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, UploadFile, File
from passlib.hash import pbkdf2_sha256
from sklearn.metrics import accuracy_score
from sqlalchemy.orm import Session

from core.credentials import API_ACCESS_TOKEN
from core.database import get_db
from core.functions.s3 import get_file_from_s3, save_file_to_s3
from core.models import schemas
from core.models.crud import crud, submits, users, hackathons

from api_versioning import versioned_api_route


router = APIRouter(route_class=versioned_api_route(2, 0))


def check_access_token(token, real_token=API_ACCESS_TOKEN):
    correct_token = secrets.compare_digest(token, real_token)
    if not correct_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect login",
        )


@router.get(
    "/user_additional_info",
    response_model=List[schemas.UserAdditionalInfo],
    summary="Получение дополнительной информации по пользователям",
    tags=["users"])
def get_user_additional_info(db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    return crud.get_user_additional_info(db=db)


@router.get(
    "/users",
    response_model=List[schemas.User],
    summary="Получение всех пользователей",
    tags=["users"])
def get_users(db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    return crud.get_users(db=db)


@router.get(
    "/submits",
    response_model=List[schemas.Submit],
    summary="Получение всех сабмитов",
    tags=["submits"])
def get_submits(db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    return crud.get_submits(db=db)


@router.get(
    "/hackathons",
    response_model=List[schemas.Hackathon],
    summary="Получение всех хакатонов",
    tags=["hackathons"])
def get_hackathons(db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    return crud.get_hackathons(db=db)


@router.get(
    "/partners",
    response_model=List[schemas.Partner],
    summary="Получение всех партнёров",
    tags=["hackathons"])
def get_partners(db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    return crud.get_partners(db=db)


@router.post(
    "/partner",
    response_model=schemas.Partner,
    summary="Создание нового партнёра",
    tags=["hackathons"],
    status_code=status.HTTP_201_CREATED)
def create_partner(partner: schemas.PartnerBase, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    return crud.create_partner(db=db, partner=partner)


@router.post(
    "/user_additional_info",
    response_model=schemas.UserAdditionalInfo,
    summary="Создание дополнительной информации о пользователе",
    tags=["users"],
    status_code=status.HTTP_201_CREATED)
def create_user_additional_info(user_additional_info: schemas.UserAdditionalInfoBase, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    return crud.create_user_additional_info(db=db, user_additional_info=user_additional_info)


@router.post(
    "/user",
    response_model=schemas.User,
    summary="Создание пользователя",
    tags=["users"],
    status_code=status.HTTP_201_CREATED)
def create_user(user: schemas.UserBase, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    user.password = pbkdf2_sha256.hash(user.password.encode('utf-8'))
    return crud.create_user(db=db, user=user)


@router.post(
    "/submit",
    response_model=schemas.Submit,
    summary="Вставка сабмита в бд",
    tags=["submits"],
    status_code=status.HTTP_201_CREATED)
def create_submit(submit: schemas.SubmitCreate, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    return crud.create_submit(db=db, submit=submit)


@router.post(
    "/hackathons",
    response_model=schemas.Hackathon,
    summary="Создание хакатона",
    tags=["hackathons"],
    status_code=status.HTTP_201_CREATED)
def create_hackathon(hackathon: schemas.HackathonBase, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    return crud.create_hackathon(db=db, hackathon=hackathon)


@router.get(
    "/leaderboard",
    response_model=List[schemas.LeaderBoard],
    summary="Получение лидерборда",
    tags=["submits"],)
def get_leaderboard(hid: int, bid: int, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    return submits.get_leaderboard(db=db, hid=hid, bid=bid)


@router.get(
    "/user_submits",
    response_model=List[schemas.UserSubmits],
    summary="Получение сабмитов пользователя",
    tags=["submits"])
def get_user_submits(hid: int, bid: int, uid: int, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    return submits.get_leaderboard(db=db, hid=hid, bid=bid, uid=uid)


@router.post(
    "/star_submit",
    response_model=schemas.SubmitReturn,
    summary="Отметить сабмит как основной",
    tags=["submits"])
def star_submit(sid: int, uid: int, action: str, db: Session = Depends(get_db), access_token: Optional[str] = ''):
    check_access_token(access_token)
    crud.logging(db=db, actions='change', object_name='', params={'sid': sid, 'action': action})
    if action == 'star':
        flags_num = submits.count_submit_star_flags(db=db, uid=uid)
        if flags_num is not None:
            if flags_num < 2:
                return submits.star_submit_star_flag(db=db, sid=sid)
            else:
                raise HTTPException(status_code=400, detail="Allowed only two stared submits")
        else:
            raise HTTPException(status_code=400, detail="Invalid values")
    else:
        return submits.unstar_submit_star_flag(db=db, sid=sid)


@router.post(
    "/user_login",
    response_model=schemas.UserLoginStatus,
    summary="Проверка почты, логина и пароля",
    tags=["login"])
def user_login(
        login_credentials: schemas.UserLogin,
        db: Session = Depends(get_db),
        access_token: Optional[str] = ''
):
    check_access_token(access_token)
    result = schemas.UserLoginStatus()
    if login_credentials.email is not None:
        result.is_email_exist = False
        email_status = users.get_emails(login_credentials=login_credentials, db=db)
        if len(email_status) > 0:
            result.is_email_exist = True
            result.is_email_unique = False
            if len(email_status) == 1:
                result.is_email_unique = True

    if result.is_email_unique and result.is_email_exist:
        if login_credentials.password is not None:
            for row in email_status:
                if pbkdf2_sha256.verify(login_credentials.password, row.password):
                    result.uid = row.id
                else:
                    result.uid = None
                    break

    if login_credentials.username is not None:
        result.is_username_exist = False
        username_status = users.get_usernames(login_credentials=login_credentials, db=db)
        if len(username_status) > 0:
            result.is_username_exist = True
            result.is_username_unique = False
            if len(username_status) == 1:
                result.is_email_unique = True

    return result


@router.post(
    "/submit_file",
    # response_model=schemas.SubmitBase,
    summary="Сохранение результатов сабмита",
    tags=["submit"])
def create_submit(
        uid: int,
        hid: int,
        bid: int,
        comment: Optional[str],
        # input_data: schemas.SubmitInput,
        background_tasks: BackgroundTasks,
        file: UploadFile = File(...),
        db: Session = Depends(get_db),
        access_token: str = '',
):
    check_access_token(access_token)
    hackathon_info = hackathons.get_hackathon_base_info(db=db, hid=hid)

    uploaded_file = np.genfromtxt(
        StringIO(file.file.read().decode('utf8')), delimiter=';', skip_header=True)
    new_file_name = file.filename.split(".")[0] + \
                    "__" + \
                    datetime.datetime.now(pytz.timezone('Europe/Moscow')).isoformat() + \
                    "." + \
                    file.filename.split(".")[1]
    file_location = f'hackathons_submits/hid{hid}/uid{uid}/bid{bid}/{new_file_name}'
    file_for_public_score = np.genfromtxt(
        StringIO(get_file_from_s3(hackathon_info.file_for_public_score).decode('utf8')),
        delimiter=';',
        skip_header=True)
    public_score = float(accuracy_score(file_for_public_score[:, 1], uploaded_file[:, 1]))

    submit = schemas.SubmitCreate(hid=hid, bid=bid, uid=uid, public_score=public_score, comment=comment, file_location=file_location, )

    def jobs_for_background():
        save_file_to_s3(file.file, file_location)

        file_for_private_score = np.genfromtxt(
            StringIO(get_file_from_s3(hackathon_info.file_for_private_score).decode('utf-8')),
            delimiter=';', skip_header=True)
        private_score = float(accuracy_score(file_for_private_score[:, 1], uploaded_file[:, 1]))
        submit.private_score = private_score
        crud.create_submit(db=db, submit=submit)

    background_tasks.add_task(jobs_for_background)

    return submit

