from sqlalchemy import Column, Integer, String,  Date,  ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base

from random import randint, random

from sqlalchemy.orm import relationship

import settings
from settings import (get_anno, YEAR_LENGTH)


Base = declarative_base()


class Position():
    PROGRESSION = 2  # основание степени (тружность получения повышения растет по степенному закону в завистмости от должности)

    CAP = len(settings.position_names) - 1

    def __init__(self, ses, human):
        self.session = ses
        self.human = human
        self.__position = 1  if self.human.start_work is None else 2

    @property
    def position(self):
        return self.__position

    def become_worker(self):
        # при начале работы надо сменить позицию с безработного на работника
        self.__position = 2

    @property
    def posname(self):
        pos = self.session.query(PosBase.name).filter(PosBase.id == self.__position).scalar()
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


class HumanFirm(Base):
    __tablename__ = 'human_firms'
    id = Column(Integer, primary_key=True)
    human_id = Column(Integer, ForeignKey('humans.id'))
    firm_id = Column(Integer, ForeignKey('firms.id'))
    move_to_firm_date = Column(Date, index=True)
    __table_args__ = (Index('ix_human_firms_human_id_firm_id', human_id, firm_id),)


class HumanPosition(Base):
    __tablename__ = 'human_positions'
    id = Column(Integer, primary_key=True)
    human_id = Column(Integer, ForeignKey('humans.id'))
    pos_id = Column(Integer, ForeignKey('positions.id'))
    move_to_position_date = Column(Date, index=True)
    __table_args__ = (Index('ix_human_positions_human_id_pos_id', human_id, pos_id),)


class Firm(Base):
    __tablename__ = 'firms'
    id = Column(Integer, primary_key=True)
    name = Column(String(70))
    rating = Column(Integer)
    open_date = Column(Date)
    ratings = relationship('FirmRating', backref='firms')


    def __init__(self, ses, name):
        self.session = ses
        self.name = name
        self.rating = self.new_rating()
        self.open_date = get_anno()

    def assign(self):
        self.session.add(FirmRating(firm=self.id, rating=self.rating, rdate=get_anno()))


    def update(self):
        if get_anno().day == 1 and get_anno().month == 1:
            self.update_rating()

    def  new_rating(self):
        return randint(10, 40)

    def update_rating(self):
        r = self.rating + randint(-4, 4)
        self.rating = max(0, r)
        self.assign()

    def __repr__(self):
        return f'<id:{self.id} "{self.name}"  рейтинг: {self.rating}>'


class FirmRating(Base):
    __tablename__ = 'firm_ratings'
    id = Column(Integer, primary_key=True)
    firm = Column(Integer, ForeignKey('firms.id'))
    rating = Column(Integer)
    rdate = Column(Date)