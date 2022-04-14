from sqlalchemy import Column, Integer, String,  Date,  ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from random import choice, randint, random
from datetime import datetime, date, timedelta

import settings
from settings import (get_rand_firm_id, get_anno, YEAR_LENGTH,
                      RETIREMENT_DELTA, RETIREMENT_MIN_AGE,
                      DEATH_DELTA, DEATH_MIN_AGE,
                      get_birthday)



Base = declarative_base()

class Position():
    PROGRESSION = 2  # основание степени (тружность получения повышения растет по степенному закону в завистмости от должности)
    POSITIONS = ['безработный', 'стажёр', 'инженер', 'старший инженер', 'главный инженер', 'начальник отдела',
                 'начальник департамента', 'директор']
    CAP = len(POSITIONS) - 1

    def __init__(self, ses, human):
        self.session = ses
        self.human = human
        self.__position = 1  if self.human.start_work is None else 2

    @property
    def position(self):
        return self.__position

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


class Human(Base):
    __tablename__ = 'humans'
    id = Column(Integer, primary_key=True)
    fname = Column(String(50))
    sname = Column(String(50))
    lname = Column(String(50))
    birth_date = Column(Date, index=True)
    talent = Column(Integer, index=True)
    start_work = Column(Date)
    firm_id = Column(Integer, ForeignKey('firms.id'), index=True)
    pos_id = Column(Integer, ForeignKey('positions.id'), index=True)
    death_date = Column(Date, index=True)
    retire_date = Column(Date, index=True)
    firm = relationship('Firm', backref='humans')

    # дата начала работы
    # стажю. От него зависит вероятность продвижения по карьернойц лестнице
    # карьерная лестница: несколько ступеней, вероятность продвиджения на следуюшую ступень меньше, чем на предыдущую (*1.5)
    # талант: влияет на вероятность повышения и на вероятность понижения
    HUM_COUNTER = 1

    def __init__(self, ses):
        self.session = ses
        self.fname = choice(settings.first_name)  #
        self.sname = choice(settings.second_name)  #
        self.lname = choice(settings.last_name)  #
        self.birth_date = get_birthday()  # день рождения
        self.talent = randint(settings.TALENT_MIN, settings.TALENT_MAX)
        self.start_work = None # сначала присваиваем None, потом вызываем функцию
        self.check_start_work()   # дата начала работы
        self.pos = Position(self.session, self) # если человек не достиг трудового возраста, он будет безработный
        self.pos_id = self.pos.position
        self.firm_id = get_rand_firm_id()

    def check_start_work(self):
        if self.start_work is None:
            # как только человеку исполняется 20 лет, он начинает работать
            if self.age < 20:
                return False
            else:
                # если человеку больше 19 лет иначе он не работает
                self.start_work =  self.birth_date + timedelta(days=366*20)
                self.migrate_record()
                return True
        else:
            return True

    def check_retirement(self):
        if self.retire_date is None:
            if self.age < RETIREMENT_MIN_AGE:
                return False
            else:
                print('Old')
                treshold = .01 + (self.age - RETIREMENT_MIN_AGE)/(DEATH_DELTA*YEAR_LENGTH)
                print(self.age, treshold)
                if random() < treshold:
                    print('Retired')
                    self.set_retired()
                    return True
        else:
            return True

    def check_death(self):
        if self.death_date is None:
            if self.age < DEATH_MIN_AGE:
                return False
            else:
                treshold = (self.age - DEATH_MIN_AGE)/(2*DEATH_DELTA*DEATH_DELTA*YEAR_LENGTH)
                if random() < treshold:
                    print('Dead')
                    self.set_dead()
                    return True
        else:
            return True


    def set_retired(self):
        self.retire_date = get_anno()

    def set_dead(self):
        self.death_date = get_anno()
        self.set_retired()

    @property
    def age(self):
        today = get_anno()
        age = today.year - self.birth_date.year - ((today.month, today.day) < (self.birth_date.month, self.birth_date.day))
        return age


    @property
    def position(self):
        return self.pos.position

    @property
    def experience(self):
        return (get_anno() - self.start_work).days

    def update(self):
        if self.check_death() is False:
            if self.check_retirement() is False:
                if self.check_start_work() is True:
                    promoted = self.pos.promotion(self.talent, self.experience)
                    if promoted:
                        self.change_position()
                    tranfered = self.migrate()
                    if tranfered:
                        self.migrate_record()

    def change_position(self):
        self.session.add(HumanPosition(human_id=self.id, pos_id=self.pos.position, move_to_position_date=get_anno()))
        self.pos_id = self.pos.position

    def migrate_record(self):
        self.session.add(HumanFirm(human_id=self.id, firm_id=self.firm_id, move_to_firm_date=get_anno()))

    def migrate(self):
        targ = get_rand_firm_id()
        if self.firm_id != targ:
            targ_firm_rating = self.session.query(Firm.rating).filter(Firm.id == targ).scalar()
            attraction_mod = targ_firm_rating - self.firm.rating
            chanse = (40 + attraction_mod) / (40 * 365)
            if random() < chanse:
                self.firm_id = targ
                return True
        return False

    def __repr__(self):
        s = f'{self.lname} {self.fname} {self.sname}, {self.birth_date}, талант:{self.talent} \
        фирма: "{self.firm.name}" долж:{self.pos.posname}, стаж: {self.experience}'
        return s


class Firm(Base):
    __tablename__ = 'firms'
    id = Column(Integer, primary_key=True)
    name = Column(String(70))
    rating = Column(Integer)
    open_date = Column(Date)

    def __init__(self, name):
        self.name = name
        self.rating = self.new_rating()
        self.open_date = get_anno()


    def update(self):
        if get_anno().day == 1 and get_anno().month == 1:
            self.update_rating()

    def  new_rating(self):
        return randint(10, 40)

    def update_rating(self):
        r = self.rating + randint(-4, 4)
        self.rating = max(0, r)

    def __repr__(self):
        return f'<id:{self.id} "{self.name}"  рейтинг: {self.rating}>'

