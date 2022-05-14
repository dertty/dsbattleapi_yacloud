from sqlalchemy import func
from sqlalchemy.orm import Session, contains_eager, load_only
from .. import models, schemas
from ..crud import crud
import datetime


def get_emails(db: Session, login_credentials: schemas.UserLogin,):
    return crud.get_users(db=db, query=True) \
        .filter(func.lower(models.User.email) == func.lower(login_credentials.email)) \
        .all()


def get_usernames(db: Session, login_credentials: schemas.UserLogin,):
    return crud.get_users(db=db, query=True) \
        .filter(func.lower(models.User.username) == func.lower(login_credentials.username)) \
        .all()


def update_nti(db, uid, nti_id):
    crud.create_user_additional_info(db, schemas.UserAdditionalInfoBase(uid=uid, key='talentkruzhokuserid', value=nti_id))


def login_nti_user(db: Session, user_details: schemas.TalentUser,):
    user_add_info = crud.get_user_additional_info(db=db, query=True) \
        .filter(models.UserAdditionalInfoOld.key == 'talentkruzhokuserid') \
        .filter(models.UserAdditionalInfoOld.value == user_details.id) \
        .all()
    if len(user_add_info) == 1:
        return crud.get_users(db=db, query=True) \
            .filter(models.UserOld.id == user_add_info[0].uid) \
            .all()[0]
    else:
        user_info = crud.get_users(db=db, query=True) \
            .filter(func.lower(models.UserOld.email) == func.lower(user_details.email)) \
            .all()
        if len(user_info) == 1:
            new_user = models.UserOld(
                id=user_info[0].id,
                email=user_info[0].email,
                firstname=user_info[0].firstname,
                lastname=user_info[0].lastname,
                reg_dt=user_info[0].reg_dt,
                last_action_dt=user_info[0].competitions,
                name=user_info[0].name,
                password=user_info[0].competitions,
                city=user_info[0].city,
                org=user_info[0].competitions,
                competitions=user_info[0].competitions,
                avatar=user_info[0].avatar,
            )
            user_additional_info_base = schemas.UserAdditionalInfoBase(
                uid=user_info[0].id,
                key='talentkruzhokuserid',
                value=user_details.id)
            crud.create_user_additional_info(db, user_additional_info_base)
            return new_user
        else:
            new_user = models.UserOld(
                email=user_details.email,
                firstname=user_details.firstname,
                lastname=user_details.lastname,
                reg_dt=datetime.datetime.now(),
                last_action_dt=datetime.datetime.now(),
                name=user_details.login,
                password=str(user_details.id) + 'password',
                city=user_details.city,
                org='NTI',
                competitions='#1#',
                avatar=user_details.avatar,
            )
            db.add(new_user)
            db.commit()
            db.refresh(new_user)

            update_nti(db, new_user.id, user_details.id)

            new_new_user = models.UserOld(
                id=new_user.id,
                email=new_user.email,
                firstname=new_user.firstname,
                lastname=new_user.lastname,
                reg_dt=new_user.reg_dt,
                last_action_dt=new_user.competitions,
                name=new_user.name,
                password=new_user.competitions,
                city=new_user.city,
                org=new_user.competitions,
                competitions=new_user.competitions,
                avatar=new_user.avatar,
            )
            # crud.create_user_additional_info(db, schemas.UserAdditionalInfoBase(uid=new_user.id, key='talentkruzhokuserid', value=user_details.id))
            return new_user


def change_name(db, uid, name):
    user = crud.get_users(db=db, query=True).get(uid)
    setattr(user, 'name', name)
    db.commit()
    db.flush()
