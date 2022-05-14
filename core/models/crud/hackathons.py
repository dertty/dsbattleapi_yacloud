from sqlalchemy.orm import Session, load_only
from .. import models
from ..crud import crud


def get_hackathon_base_info(db: Session, hid: int,):
    return crud.get_hackathons(db=db, query=True) \
        .filter(models.Hackathon.id == hid) \
        .options(
            load_only('status', 'file_for_public_score', 'file_for_private_score')) \
        .first()
