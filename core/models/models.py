from core.database import Base

from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, Float, VARCHAR, DateTime, ForeignKey, BOOLEAN, CHAR


class Hackathon(Base):
    __tablename__ = "hackathons"

    id = Column(Integer, primary_key=True, index=True)
    status = Column(VARCHAR(256), nullable=False)
    title = Column(VARCHAR(256), nullable=False)
    description = Column(VARCHAR(256), nullable=False)
    file_for_public_score = Column(VARCHAR(256), nullable=True)
    file_for_private_score = Column(VARCHAR(256), nullable=True)

    hackathon_partners_info = relationship("HackathonPartnersInfo", backref="hackathons")
    submits = relationship("Submit", backref="hackathons")


class Partner(Base):
    __tablename__ = "partners"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(VARCHAR(256), nullable=True)
    link = Column(VARCHAR(256), nullable=True)
    logo = Column(VARCHAR(256), nullable=True)


class HackathonPartnersInfo(Base):
    __tablename__ = "hackathon_partners_info"

    id = Column(Integer, primary_key=True, index=True)
    hid = Column(Integer, ForeignKey('hackathons.id'))
    partner_id = Column(Integer, ForeignKey('partners.id'))

    partner = relationship("Partner", backref="partners", uselist=False)


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    firstname = Column(VARCHAR(256), nullable=False)
    lastname = Column(VARCHAR(256), nullable=False)
    username = Column(VARCHAR(256), nullable=False)
    email = Column(VARCHAR(256), nullable=False)
    password = Column(CHAR(96), nullable=False)

    additional_info = relationship("UserAdditionalInfo", backref="users")


class Submit(Base):
    __tablename__ = "submits"

    id = Column(Integer, primary_key=True, index=True)
    uid = Column(Integer, ForeignKey('users.id'))
    hid = Column(Integer, ForeignKey('hackathons.id'))
    bid = Column(Integer, nullable=False, index=True)
    submit_dt = Column(DateTime, nullable=False)
    file_location = Column(VARCHAR(256), nullable=False)

    private_score = Column(Float, nullable=False, default=0)
    public_score = Column(Float, nullable=False, default=0)
    comment = Column(VARCHAR(256), nullable=True)
    stared_flg = Column(BOOLEAN, nullable=False, default=False)

    user = relationship("User", backref="submits")


class UserAdditionalInfo(Base):
    __tablename__ = "user_additional_info"

    id = Column(Integer, primary_key=True)
    uid = Column(Integer, ForeignKey('users.id'))
    key = Column(VARCHAR(256), nullable=False)
    value = Column(VARCHAR(256), nullable=False)
    add_dt = Column(DateTime, nullable=False)


class Log(Base):
    __tablename__ = "logs"

    id = Column(Integer, primary_key=True)
    log = Column(VARCHAR(256), nullable=False)
    add_dt = Column(DateTime, nullable=False)
    uid = Column(Integer, nullable=True)