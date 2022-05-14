from core.database import Base

from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, Float, VARCHAR, DateTime, ForeignKey, BOOLEAN, CHAR


# class Hackathon(Base):
#     __tablename__ = "hackathons"
#
#     id = Column(Integer, primary_key=True, index=True)
#     status = Column(VARCHAR(256), nullable=False)
#     title = Column(VARCHAR(256), nullable=False)
#     description = Column(VARCHAR(256), nullable=False)
#     file_for_public_score = Column(VARCHAR(256), nullable=True)
#     file_for_private_score = Column(VARCHAR(256), nullable=True)
#
#     hackathon_partners_info = relationship("HackathonPartnersInfo", backref="hackathons")
#     submits = relationship("Submit", backref="hackathons")
#
#
# class Partner(Base):
#     __tablename__ = "partners"
#
#     id = Column(Integer, primary_key=True, index=True)
#     name = Column(VARCHAR(256), nullable=True)
#     link = Column(VARCHAR(256), nullable=True)
#     logo = Column(VARCHAR(256), nullable=True)
#
#
# class HackathonPartnersInfo(Base):
#     __tablename__ = "hackathon_partners_info"
#
#     id = Column(Integer, primary_key=True, index=True)
#     hid = Column(Integer, ForeignKey('hackathons.id'))
#     partner_id = Column(Integer, ForeignKey('partners.id'))
#
#     partner = relationship("Partner", backref="partners", uselist=False)


class UserOld(Base):
    __tablename__ = "dsbt_users"

    id = Column(Integer, primary_key=True, index=True)
    reg_dt = Column(DateTime, nullable=False)
    org = Column(VARCHAR(256), nullable=False)
    avatar = Column(VARCHAR(256), nullable=False)
    competitions = Column(VARCHAR(256), nullable=False, default='')
    wins = Column(Integer, nullable=False, default=0)
    best = Column(Float, nullable=False, default=0)
    city = Column(VARCHAR(256), nullable=False)
    last_action_dt = Column(DateTime, nullable=False)
    firstname = Column(VARCHAR(256), nullable=False)
    lastname = Column(VARCHAR(256), nullable=False)
    name = Column(VARCHAR(256), nullable=False)
    email = Column(VARCHAR(256), nullable=False)
    password = Column(CHAR(96), nullable=False)


class SubmitOld(Base):
    __tablename__ = "dsbt_submits"

    id = Column(Integer, primary_key=True, index=True)
    hid = Column(Integer, nullable=False, index=True)
    bid = Column(Integer, nullable=False, index=True)
    uid = Column(Integer, nullable=False, index=True)
    submit_dt = Column(DateTime, nullable=False)

    file_location = Column(VARCHAR(256), nullable=False, default=None)

    private_score = Column(Float, nullable=False, default=0)
    public_score = Column(Float, nullable=False, default=0)
    score = Column(Float, nullable=False, default=0)
    private_score_1 = Column(Float, nullable=False, default=0)
    public_score_1 = Column(Float, nullable=False, default=0)
    score_1 = Column(Float, nullable=False, default=0)
    comment = Column(VARCHAR(256), nullable=True, default=None)
    stared_flg = Column(BOOLEAN, nullable=False, default=False)

    precission = Column(Float, nullable=False, default=0)
    recall = Column(Float, nullable=False, default=0)
    f_measure = Column(Float, nullable=False, default=0)


class UserAdditionalInfoOld(Base):
    __tablename__ = "dsbt_user_additional_info"

    id = Column(Integer, primary_key=True, index=True)
    uid = Column(Integer, ForeignKey('dsbt_users.id'))
    key = Column(VARCHAR(256), nullable=False)
    value = Column(Integer, nullable=False)



class TeamOld(Base):
    __tablename__ = "dsbt_teams"

    id = Column(Integer, primary_key=True)
    name = Column(VARCHAR(256), nullable=False)
    hid = Column(Integer, nullable=False)
    bid = Column(Integer, nullable=False)
    owner_id = Column(Integer, nullable=False)
    active = Column(Integer, nullable=False, default=1)


class TeamParticipantsOld(Base):
    __tablename__ = "dsbt_team_participants"

    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, nullable=False)
    user_id = Column(Integer, nullable=False)
    status = Column(Integer, nullable=False)

#
#
# class Log(Base):
#     __tablename__ = "logs"
#
#     id = Column(Integer, primary_key=True)
#     log = Column(VARCHAR(256), nullable=False)
#     add_dt = Column(DateTime, nullable=False)
#     uid = Column(Integer, nullable=True)
