from typing import Optional
from sqlalchemy import Column, Integer, String,  Date, Boolean,  ForeignKey, Index

from random import random

from sqlalchemy.orm import relationship

from model import Base, session
import settings
from settings import (get_anno,
                      YEAR_LENGTH)




class Position():
    PROGRESSION = 2  # основание степени (тружность получения повышения растет по степенному закону в завистмости от должности)

    CAP = len(settings.position_names)

    def __init__(self, human: 'People', initial_position:  Optional[int] = None):

        self.human = human
        if initial_position is None:
            self.__position = 1
        else:
            self.__position = initial_position

    @property
    def position(self):
        return self.__position

    def set_position(self, value):
        self.__position = value

    def become_worker(self):
        # при начале работы надо сменить позицию с безработного на работника
        self.__position = 2

    def become_director(self):
        # при начале работы надо сменить позицию с безработного на работника
        self.__position = Position.CAP


    @property
    def posname(self):
        pos = session.query(PosBase.name).filter(PosBase.id == self.__position).scalar()
        return pos

    # повышение по службе
    def promotion(self, talent, experience):
        # переводим человека из безработынх на его первую должность
        if self.human.start_work is not None and self.__position == 1:
            self.__position = 2
        # шанс на повышение
        # зависит от трудового опыта - чем больше стаж человека, тем больше шанс повышения
        # от таланта: чем больше талант, тем легче получит повышение
        # и от занимаемой должности: шанс перейти на следующую ступень в два раза меньше
        if self.__position < Position.CAP:
            x = random()
            # отнимаю от позиции единицу, чтобы безработные не увеличивали степень в формуле
            base_mod = 1 / (YEAR_LENGTH * Position.PROGRESSION ** (self.__position-1))
            # умножает время перехода на следующую должность. Для умных время до повыщения сокращается
            chisl = (2 * settings.TALENT_MAX + talent)
            talent_mod = chisl / settings.TALENT_RANGE
            experience_mod = (1 + settings.EXPERIENCE_CAP * experience)
            if x < base_mod * talent_mod * experience_mod:
                self.__position += 1
                return True
            return False

    def demotion(self):  # понижние по службе
        pass

class LastSimDate(Base):
    __tablename__ = 'last_sim_date'
    id = Column(Integer, primary_key=True)
    date = Column(Date, default= get_anno(), onupdate=get_anno())


class PosBase(Base):
    __tablename__ = 'positions'
    id = Column(Integer, primary_key=True)
    name = Column(String(70))


class PeopleFirm(Base):
    __tablename__ = 'people_firms'
    id = Column(Integer, primary_key=True)
    people_id = Column(Integer, ForeignKey('people.id'))
    firm_id = Column(Integer, ForeignKey('firms.id'))
    move_to_firm_date = Column(Date, index=True)
    __table_args__ = (Index('ix_people_firms_people_id_firm_id', people_id, firm_id),)
    human_conn = relationship('People', back_populates='worked_in_firms')
    firm_conn = relationship('Firm', back_populates='people')

class PeoplePosition(Base):
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

class FirmRating(Base):
    __tablename__ = 'firm_ratings'
    id = Column(Integer, primary_key=True)
    firm_id = Column(Integer, ForeignKey('firms.id'))
    rating = Column(Integer)
    rdate = Column(Date)