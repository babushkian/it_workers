from datetime import timedelta, date
from random import choice, randint, random

from sqlalchemy import Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import relationship

import settings
from model.worker_base import (Base,
                               Position,
                               PeoplePosition,
                               PeopleFirm,
                               Firm,
                               LastSimDate)
from settings import (get_birthday,
                      get_anno,
                      RETIREMENT_MIN_AGE,
                      RETIREMENT_DELTA,
                      DEATH_DELTA,
                      YEAR_LENGTH,
                      DEATH_MIN_AGE)


class People(Base):
    __tablename__ = 'people'
    id = Column(Integer, primary_key=True)
    first_name = Column(String(50))
    second_name = Column(String(50))
    last_name = Column(String(50))
    birth_date = Column(Date, index=True)
    talent = Column(Integer, index=True)
    start_work = Column(Date)
    last_firm_id = Column(Integer, ForeignKey('firms.id'), index=True)
    last_position_id = Column(Integer, ForeignKey('positions.id'), index=True)
    death_date = Column(Date, index=True)
    retire_date = Column(Date, index=True)
    recent_firm = relationship('Firm', back_populates='recent_emploees')
    worked_in_firms = relationship('Firm', secondary='people_firms' )

    position = relationship('PeoplePosition', backref='humans')
    position_name = relationship('PosBase', backref='humans')


    # дата начала работы
    # стаж. От него зависит вероятность продвижения по карьерной лестнице
    # карьерная лестница: несколько ступеней, вероятность продвиджения на следуюшую ступень меньше, чем на предыдущую
    # талант: влияет на вероятность повышения и на вероятность понижения

    def __init__(self, ses):
        self.session = ses
        self.first_name = choice(settings.first_name)  #
        self.second_name = choice(settings.second_name)  #
        self.last_name = choice(settings.last_name)  #
        self.birth_date = get_birthday()  # день рождения
        self.talent = randint(settings.TALENT_MIN, settings.TALENT_MAX)
        self.start_work = None # сначала присваиваем None, потом вызываем функцию


    def assign(self):
        '''
        при инициации нужно присвоить человеку какую-то должность. Делает ся это через таблицу people_positions
        но из инита People сделать запись в нее нельзя, та как у People  в этот момент еще не определен id
        '''
        self.last_firm_id = Firm.get_rand_firm_id()

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
        if self.retire_date is not None:
            return True
        elif self.age < RETIREMENT_MIN_AGE:
                return False
        else:
            treshold =  (self.age + 1 - RETIREMENT_MIN_AGE)/(RETIREMENT_DELTA*YEAR_LENGTH)
            if random() < treshold:
                print('Retired')
                self.set_retired()
                return True
            else:
                return False


    def check_death(self):
        if self.death_date is not  None: # уже умер
            return True
        elif self.age < DEATH_MIN_AGE: # возраст слишком ранний для умирания
            return False
        else: # Есть возможность умереть
            treshold = (self.age - DEATH_MIN_AGE)/(18*DEATH_DELTA*YEAR_LENGTH)
            if random() < treshold: # повезло, умер
                print('Dead')
                self.set_dead()
                return True
            else:
                return False



    def set_retired(self):
        self.retire_date = get_anno()

    def set_dead(self):
        self.death_date = get_anno()
        if self.retire_date is None:
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
                if self.check_start_work():
                    promoted = self.pos.promotion(self.talent, self.experience)
                    if promoted:
                        self.change_position()
                    tranfered = self.migrate()
                    if tranfered:
                        self.migrate_record()

    def change_position(self):
        self.last_position_id = self.pos.position
        self.session.add(PeoplePosition(people_id=self.id, position_id=self.last_position_id, move_to_position_date=get_anno()))

    def migrate_record(self):
        self.session.add(PeopleFirm(people_id=self.id, firm_id=self.last_firm_id, move_to_firm_date=get_anno()))

    def migrate(self):
        '''
        Переходим в другую фирму
        '''
        targ = Firm.get_rand_firm_id()
        if self.last_firm_id != targ:
            targ_firm_rating = self.session.query(Firm.last_rating).filter(Firm.id == targ).scalar()
            attraction_mod = targ_firm_rating - self.recent_firm.last_rating
            chanse = (40 + attraction_mod) / (40 * 365)
            if random() < chanse:
                self.last_firm_id = targ
                return True
        return False

    def __repr__(self):
        s = f'id: {self.id} {self.last_name} {self.first_name} {self.second_name}, {self.birth_date}, талант:{self.talent} \
        фирма: "{self.firm.firmname.name}" долж: "{self.position_name.name}"  нач. работы: {self.start_work}'
        return s