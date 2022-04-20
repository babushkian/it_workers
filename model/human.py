from datetime import timedelta, date
from random import choice, randint, random

from sqlalchemy import Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import relationship

import settings
from model.worker_base import (Base,
                               Position,
                               HumanPosition,
                               HumanFirm,
                               Firm)
from settings import (get_birthday,
                      get_rand_firm_id,
                      get_anno,
                      RETIREMENT_MIN_AGE,
                      DEATH_DELTA,
                      YEAR_LENGTH,
                      DEATH_MIN_AGE)


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
    position = relationship('HumanPosition', backref='humans')
    position_name = relationship('PosBase', backref='humans')

    # дата начала работы
    # стаж. От него зависит вероятность продвижения по карьерной лестнице
    # карьерная лестница: несколько ступеней, вероятность продвиджения на следуюшую ступень меньше, чем на предыдущую
    # талант: влияет на вероятность повышения и на вероятность понижения

    def __init__(self, ses):
        self.session = ses
        self.fname = choice(settings.first_name)  #
        self.sname = choice(settings.second_name)  #
        self.lname = choice(settings.last_name)  #
        self.birth_date = get_birthday()  # день рождения
        self.talent = randint(settings.TALENT_MIN, settings.TALENT_MAX)
        self.start_work = None # сначала присваиваем None, потом вызываем функцию
        # self.pos = Position(self.session, self) # если человек не достиг трудового возраста, он будет безработный
        # self.pos_id = self.pos.position
        # self.firm_id = get_rand_firm_id()


    def assign(self):
        '''
        при инициации нужно присвоить человеку какую-то должность. Делает ся это через таблицу human_positions
        но из инита Human сделать запись в нее нельзя, та как у Human  в этот момент еще не определен id
        '''
        self.firm_id = get_rand_firm_id()
        self.initial_check_start_work()
        self.pos = Position(self.session, self) # если человек не достиг трудового возраста, он будет безработный
        self.change_position()


    def initial_check_start_work(self):
        # как только человеку исполняется 20 лет, он начинает работать
        y = self.birth_date.year + 20
        anniversary_20 = date(year = y, month=self.birth_date.month, day=self.birth_date.day)
        if anniversary_20 <= get_anno():
            self.start_work =  anniversary_20
            # определили, что человек работает
            # а раз работает, сразу делаем запись что с сегодняшнего для он трудоустроен
            # в фирме, айдишник которой выпал при первоначальной генерации
            self.migrate_record()

    def check_start_work(self):
        if self.start_work is None:
            # как только человеку исполняется 20 лет, он начинает работать
            if self.age < 20:
                return False
            else:
                # если человеку больше 19 лет иначе он не работает
                self.start_work = get_anno()
                self.pos.become_worker() # повышаем с безработного жо первой ступени работника
                self.change_position() # делаем запись о смене позиции
                self.migrate_record() # делаем запись о начале работы в фирме
                return True
        else:
            return True

    def check_retirement(self):
        if self.id > 87 and self.id < 90:
            print(f'{self.id=} {self.age=} {get_anno().year - self.birth_date.year} {RETIREMENT_MIN_AGE=}')

        if self.retire_date is None:
            if self.age < RETIREMENT_MIN_AGE:
                return False
            else:
                print('Old')
                treshold = .1 + (self.age - RETIREMENT_MIN_AGE)/(DEATH_DELTA*YEAR_LENGTH)
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
        self.pos_id = self.pos.position
        self.session.add(HumanPosition(human_id=self.id, pos_id=self.pos_id, move_to_position_date=get_anno()))

    def migrate_record(self):
        self.session.add(HumanFirm(human_id=self.id, firm_id=self.firm_id, move_to_firm_date=get_anno()))

    def migrate(self):
        '''
        Переходим в другую фирму
        '''
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
        s = f'id: {self.id} {self.lname} {self.fname} {self.sname}, {self.birth_date}, талант:{self.talent} \
        фирма: "{self.firm.name}" долж:{self.position_name.name}, стаж: {self.experience}'
        return s