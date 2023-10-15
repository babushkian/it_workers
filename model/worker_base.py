from datetime import date
from typing import Optional
from sqlalchemy import Column, Integer, String,  Date, Boolean,  ForeignKey, Index

from random import random

from sqlalchemy.orm import relationship, mapped_column, Mapped

from model import Base, session
import settings
from settings import (get_anno,
                      YEAR_LENGTH,
                      POSITION_CAP,)




class Position():

    """
    Этот класс не ассоциирован с какой-либо таблицей. Это свойство у класса People,
    показывающее текущую позицию (должность) человека
    """
    # основание степени (трудность получения повышения растет по степенному закону в завистмости от должности)
    PROGRESSION = 2
    def __init__(self, human: 'People', initial_position: None|int = None):

        self.human = human
        if initial_position is None:
            self.__position = 1
        else:
            self.__position = initial_position

    @property
    def position(self):
        return self.__position

    @position.setter
    def position(self, value):
        self.__position = value
        self.human.current_position_id = self.__position
        session.add(PeoplePosition(
            people_id=self.human.id,
            position_id=self.__position,
            move_to_position_date=get_anno()))

    def set_position_unemployed(self):
        self.position = 1

    def set_position_employed(self):
        self.position = 2

    def set_position_director(self):
        self.position = POSITION_CAP



    @property
    def position_name(self) -> str:
        pos: str = session.query(PositionNames.name).filter(PositionNames.id == self.position).scalar()
        return pos

    def promote(self):
        self.position = self.position + 1

    # повышение по службе
    def promotion_attempt(self) -> bool:
        # шанс на повышение
        # зависит от трудового опыта - чем больше стаж человека, тем больше шанс повышения
        # от таланта: чем больше талант, тем легче получит повышение
        # и от занимаемой должности: шанс перейти на следующую ступень в два раза меньше
        if self.position < POSITION_CAP:
            x = random()
            # отнимаю от позиции единицу, чтобы безработные не увеличивали степень в формуле
            base_mod = 1 / (YEAR_LENGTH * Position.PROGRESSION ** (self.position-1))
            # умножает время перехода на следующую должность. Для умных время до повыщения сокращается
            chisl = (2 * settings.TRAIT_MAX + self.human.talent)
            talent_mod = chisl / settings.TRAIT_RANGE
            experience_mod = (1 + settings.EXPERIENCE_CAP * self.human.experience)
            if x < base_mod * talent_mod * experience_mod:
                self.promote()
                return True
        return False

class LastSimDate(Base):
    __tablename__ = 'last_sim_date'
    id = Column(Integer, primary_key=True)
    date = Column(Date, default= get_anno(), onupdate=get_anno())


class PositionNames(Base):
    """Словарь названий позиций"""
    __tablename__ = 'positions'
    id = Column(Integer, primary_key=True)
    name = Column(String(70))


class PeopleFirm(Base):
    """Таблица показывает, в каких фирмах работали люди. Из какой фирмы пришли и в какой день
    устроились на новое место"""
    __tablename__ = 'people_firms'
    id = Column(Integer, primary_key=True)
    people_id = Column(Integer, ForeignKey('people.id'))
    firm_from_id = Column(Integer)

    firm_to_id = Column(Integer, ForeignKey('firms.id'))
    move_to_firm_date = Column(Date, index=True)
    __table_args__ = (Index('ix_people_firms_people_id_firm_id', people_id, firm_to_id),)
    human_conn = relationship('People', back_populates='worked_in_firms')
    firm_conn = relationship('Firm', back_populates='people')

    def __repr__(self):
        return f"<PeopleFirm people_id= {self.people_id} from_firm= {self.firm_from_id} to_firm={self.firm_to_id} date {self.move_to_firm_date}>"

class PeoplePosition(Base):
    """
    Промежуточная таблица, связывающая людей и их должности
    """

    __tablename__ = 'people_positions'
    id = Column(Integer, primary_key=True)
    people_id = Column(Integer, ForeignKey('people.id'))
    position_id = Column(Integer, ForeignKey('positions.id'))
    move_to_position_date = Column(Date, index=True)
    __table_args__ = (Index('ix_people_positions_people_id_pos_id', people_id, position_id),)


class FirmName(Base):
    __tablename__ = 'firmnames'
    id = Column(Integer, primary_key=True)
    name = Column(String(70))
    used = Column(Boolean, default=False)
    firm_with_name = relationship('Firm', back_populates='firmname', uselist=False, innerjoin=True)

    def __repr__(self):
        return f'<FirmName  id:{self.id} "{self.name}"  used: {self.used}>'

class FirmRating(Base):
    __tablename__ = 'firm_ratings'
    id = Column(Integer, primary_key=True)
    firm_id = Column(Integer, ForeignKey('firms.id'))
    rating = Column(Integer)
    workers_count = Column(Integer)
    rate_date = Column(Date)

