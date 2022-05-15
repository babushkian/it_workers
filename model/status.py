from typing import Optional
from sqlalchemy import Column, Integer, String,  Date, Boolean,  ForeignKey, Index
from enum import Enum


from sqlalchemy.orm import relationship

from model import Base, session

from settings import (get_anno,
                      )


class Status(str, Enum):
    UNKNOWN = 1
    YONG = 2
    UNEMPLOYED = 3
    EMPLOYED = 4
    VACATION = 5
    ILL = 6
    RETIRED = 7
    DEAD = 8

statnames = {
    Status.UNKNOWN: 'неопределенный',
    Status.YONG: 'молод ещё',
    Status.UNEMPLOYED: 'безработный',
    Status.EMPLOYED: 'работает',
    Status.VACATION: 'в отпуске',
    Status.ILL: 'на больничном',
    Status.RETIRED: 'на пенсии',
    Status.DEAD: 'умер',
}

class StatusName(Base):
    __tablename__ = 'statuses'
    id = Column(Integer, primary_key=True)
    name = Column(String)

class PeopleStatus(Base):
    __tablename__ = 'people_status'
    id = Column(Integer, primary_key=True)
    people_id = Column(Integer, ForeignKey('people.id'))
    status_id = Column(Integer, ForeignKey('statuses.id'))
    status_date = Column(Date, index=True)
    __table_args__ = (Index('ix_people_status_people_id_status_id', people_id, status_id),)
    status_name = relationship('StatusName')

class StatHandle():
    def __init__(self, hum):
        self.human = hum
        self.__status = Status.UNKNOWN


    @property
    def status(self):
        return self.__status.value



    def status_record(self):
        session.add(PeopleStatus(people_id=self.human.id,
                                 status_id=self.status,
                                 status_date=get_anno()))

    def set_status(self, status):
        self.__status = status
        self.status_record()


    def set_status_young(self):
        self.set_status(Status.YONG)

    def set_status_unemployed(self):
        self.set_status(Status.UNEMPLOYED)

    def set_status_employed(self):
        self.set_status(Status.EMPLOYED)

    def set_status_ill(self):
        self.set_status(Status.ILL)

    def set_status_retired(self):
        self.set_status(Status.RETIRED)

    def set_status_dead(self):
        self.set_status(Status.DEAD)

