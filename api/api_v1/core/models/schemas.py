from pydantic import BaseModel, Field, StrictBool
from typing import Optional, List
import datetime


class SuccessRequest:
    success: StrictBool


#####################################################
# Log
#####################################################
class LogBase(BaseModel):
    log: str
    add_dt: datetime.datetime
    uid: Optional[int] = None


class Log(LogBase):
    id: int

    class Config:
        orm_mode = True


#####################################################
# Partner logo and link
#####################################################
class PartnerBase(BaseModel):
    # hid: int = Field(..., description="The id of hackathon for which partner are added")
    name: Optional[str] = Field(None, title="The name of the partner", max_length=256, example='Partner name')
    logo: Optional[str] = Field(None, title="The link to logo of partner", max_length=256, example='/logo.png')
    link: Optional[str] = Field(None, title="The link to partner", max_length=256, example='https://partner.com')


class Partner(PartnerBase):
    id: int

    class Config:
        orm_mode = True


#####################################################
# User
#####################################################
class UserLogin(BaseModel):
    email: str = Field(None, title="The user's email", max_length=256, example='aleksandr@email.com')
    password: str = Field(None, title="The user's password hash", max_length=256, example='%U5KVM"8~:7w{xFN')
    username: str = Field(None, title="The user's account name - pseudonym", max_length=256, example='Aleksandr_Ivanov')


class UserBase(UserLogin):
    firstname: str = Field(..., title="The user's firstname", max_length=256, example='Aleksandr')
    lastname: str = Field(..., title="The user's lastname", max_length=256, example='Ivanov')
    username: str = Field(..., title="The user's account name - pseudonym", max_length=256, example='Aleksandr_Ivanov')
    email: str = Field(..., title="The user's email", max_length=256, example='aleksandr@email.com')
    password: str = Field(..., title="The user's password hash", max_length=256, example='%U5KVM"8~:7w{xFN')


class User(UserBase):
    id: int
    additional_info: Optional[List[Partner]] = []

    class Config:
        orm_mode = True


class UserLoginStatus(BaseModel):
    uid: Optional[int] = None
    is_email_exist: Optional[bool] = None
    is_email_unique: Optional[bool] = None
    is_username_exist: Optional[bool] = None
    is_username_unique: Optional[bool] = None


#####################################################
# Submit
#####################################################
class SubmitInput(BaseModel):
    uid: int
    hid: int
    bid: int
    comment: Optional[str] = None


class SubmitBase(BaseModel):
    public_score: float = 0
    public_score_1: float = 0
    stared_flg: bool = False
    comment: Optional[str] = None
    file_location: str


class SubmitCreate(SubmitBase):
    uid: int
    hid: int
    bid: int
    private_score: float = 0
    private_score_1: float = 0
    score: float = 0
    score_1: float = 0


class Submit(SubmitCreate):
    id: int
    submit_dt: datetime.datetime

    user: User

    class Config:
        orm_mode = True


class SubmitReturn(SubmitBase):
    id: int
    submit_dt: datetime.datetime

    class Config:
        orm_mode = True


#####################################################
# User additional info
#####################################################
class UserAdditionalInfoBase(BaseModel):
    uid: int
    key: str
    value: int

    class Config:
        orm_mode = True


# class UserAdditionalInfo(UserAdditionalInfoBase):
#     id: int
#     add_dt: datetime.datetime
#
#     class Config:
#         orm_mode = True


#####################################################
# Hackathon partners info
#####################################################
class HackathonPartnersInfoBase(BaseModel):
    hid: int
    partner_id: int


class HackathonPartnersInfo(HackathonPartnersInfoBase):
    id: int
    partner: Partner

    class Config:
        orm_mode = True


#####################################################
# Hackathon
#####################################################
class HackathonBase(BaseModel):
    title: str = Field(..., title="The title of the hackathon", max_length=256, example='Hackathon')
    description: str = Field(..., title="The description of the hackathon", max_length=256, example='Hackathon for students')
    status: str = Field(..., title="The status of the hackathon", max_length=256, example='active')
    file_for_public_score: Optional[str] = Field(None, title="The path to the public dataset", max_length=256, example='/public_test.csv')
    file_for_private_score: Optional[str] = Field(None, title="The path to the private dataset", max_length=256, example='/private_test.csv')


class Hackathon(HackathonBase):
    id: int
    hackathon_partners_info: Optional[List[HackathonPartnersInfo]] = []
    submits: Optional[List[Submit]] = []

    class Config:
        orm_mode = True


class LeaderBoard(BaseModel):
    rank: int
    name: str
    public_score: str

    class Config:
        orm_mode = True

class FinalLeaderBoard(BaseModel):
    rank: int
    name: str
    email: str
    firstname: str
    lastname: str
    city: str
    public_score: str
    private_score: str
    submit_dt: datetime.datetime

    class Config:
        orm_mode = True


class UserSubmits(BaseModel):
    id: int
    rank: int
    public_score: str
    submit_dt: datetime.datetime
    stared_flg: bool
    comment: Optional[str]
    file_location: Optional[str]

    class Config:
        orm_mode = True


class MaxScore(BaseModel):
    max_score: float = 0
    submits_num: int = 0

    class Config:
        orm_mode = True


class TalentUser(BaseModel):
    id: int
    login: str
    email: str
    firstname: str = ''
    lastname: str = ''
    city: str = ''
    avatar: str = ''

    class Config:
        orm_mode = True


class UserInfo(BaseModel):
    user_sender_id: int
    id: int
    name: Optional[str] = None
    email: Optional[str] = None
    firstname: Optional[str] = None
    lastname: Optional[str] = None
    city: Optional[str] = None
    avatar: Optional[str] = None

    class Config:
        orm_mode = True


class TeamCreate(BaseModel):
    name: str
    hid: int
    bid: int
    owner_id: int


class Team(TeamCreate):
    id: int

    class Config:
        orm_mode = True


class TeamInvite(BaseModel):
    owner_id: int
    team_id: int
    participant_email: str


class TeamInviteApprove(BaseModel):
    id: int
    user_id: int


class TeamInviteDelete(BaseModel):
    user_id: int
    invite_id: Optional[int] = None
    team_id: Optional[int] = None


class TeamDelete(BaseModel):
    user_id: int
    team_id: int


class HackathonStats(BaseModel):
    dt: str
    users_num: int = 0
    active_users_num: int = 0
    submits_num: int = 0

    class Config:
        orm_mode = True
