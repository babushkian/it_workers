from sqlalchemy import Column, Integer, String,  Date, Boolean,  ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base

from random import randint, random, choice

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


class PeopleFirm(Base):
    __tablename__ = 'people_firms'
    id = Column(Integer, primary_key=True)
    people_id = Column(Integer, ForeignKey('people.id'))
    firm_id = Column(Integer, ForeignKey('firms.id'))
    move_to_firm_date = Column(Date, index=True)
    __table_args__ = (Index('ix_people_firms_people_id_firm_id', people_id, firm_id),)


class PeoplePosition(Base):
    __tablename__ = 'people_positions'
    id = Column(Integer, primary_key=True)
    people_id = Column(Integer, ForeignKey('people.id'))
    position_id = Column(Integer, ForeignKey('positions.id'))
    move_to_position_date = Column(Date, index=True)
    __table_args__ = (Index('ix_people_positions_people_id_pos_id', people_id, position_id),)


class Firm(Base):
    session = None
    __tablename__ = 'firms'
    id = Column(Integer, primary_key=True)
    firmname_id = Column(Integer, ForeignKey('firmnames.id'))
    last_rating = Column(Integer)
    open_date = Column(Date)
    close_date = Column(Date)
    ratings = relationship('FirmRating', backref='firms')
    firmname = relationship('FirmName', uselist=False, back_populates="firm_with_name", innerjoin=True)
    recent_emploees = relationship('People', back_populates='recent_firm')
    #people = relationship('People', secondary='human_firms')

    def __init__(self, n):
        self.firmname_id = n
        self.last_rating = self.new_rating()
        self.open_date = get_anno()

    @classmethod
    def bind_session(cls, session):
        cls.session = session

    @classmethod
    def get_rand_firm_id(cls):
        pool = cls.session.query(Firm.id).filter(Firm.close_date.is_(None)).all()
        return choice(pool)[0]

    @classmethod
    def get_used_firm_ids_pool(cls):
        pool = cls.session.query(FirmName.id).filter(FirmName.used==True).all()
        return pool

    @classmethod
    def get_unused_firmname_id(cls):
        pool = cls.session.query(FirmName.id).filter(FirmName.used==False).all()
        assert len(pool) > 0, 'нет свободных названий фирм'
        new_id = choice(pool)[0]
        cls.mark_firmname_as_used(new_id)
        return new_id

    @classmethod
    def mark_firmname_as_used(cls, new_id):
        cls.session.query(FirmName).filter(FirmName.id == new_id).update({'used':True})

    def assign(self):
        Firm.session.add(FirmRating(firm_id=self.id, rating=self.last_rating, rdate=get_anno()))


    def update(self):
        if get_anno().day == 1 and get_anno().month == 1:
            self.update_rating()

    def  new_rating(self):
        return randint(10, 40)

    def update_rating(self):
        r = self.last_rating + randint(-4, 4)
        self.last_rating = max(0, r)
        self.assign()

    def __repr__(self):
        return f'<id:{self.id} "{self.firmname.name}"  рейтинг: {self.last_rating}>'

class FirmName(Base):
    __tablename__ = 'firmnames'
    id = Column(Integer, primary_key=True)
    name = Column(String(70))
    used = Column(Boolean, default=False)
    firm_with_name = relationship('Firm', uselist=False, back_populates='firmname')

class FirmRating(Base):
    __tablename__ = 'firm_ratings'
    id = Column(Integer, primary_key=True)
    firm_id = Column(Integer, ForeignKey('firms.id'))
    rating = Column(Integer)
    rdate = Column(Date)