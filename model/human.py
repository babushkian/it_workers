from typing import Optional

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
                      DEATH_MIN_AGE,
                      UNEMPLOYED,)


class People(Base):
    session = None
    __tablename__ = 'people'
    id = Column(Integer, primary_key=True)
    first_name = Column(String(50))
    second_name = Column(String(50))
    last_name = Column(String(50))
    birth_date = Column(Date, index=True)
    talent = Column(Integer, index=True)
    start_work = Column(Date)
    current_firm_id = Column(Integer, ForeignKey('firms.id'), default= 1, nullable=False, index=True)
    last_position_id = Column(Integer, ForeignKey('positions.id'),  index=True)
    death_date = Column(Date, index=True)
    retire_date = Column(Date, index=True)

    recent_firm = relationship('Firm', back_populates='recent_emploees')
    # у человека всегда есть позиция, поэтому LEFT OUTER JOIN не требуется
    position_name = relationship('PosBase', innerjoin=True)
    worked_in_firms = relationship('PeopleFirm', back_populates='human_conn')

    obj_firms = None

    # дата начала работы
    # стаж. От него зависит вероятность продвижения по карьерной лестнице
    # карьерная лестница: несколько ступеней, вероятность продвиджения на следуюшую ступень меньше,
    # чем на предыдущую
    # талант: влияет на вероятность повышения и на вероятность понижения

    def __init__(self):
        self.first_name = choice(settings.first_name)  #
        self.second_name = choice(settings.second_name)  #
        self.last_name = choice(settings.last_name)  #
        self.birth_date = get_birthday()  # день рождения
        self.talent = randint(settings.TALENT_MIN, settings.TALENT_MAX)
        self.start_work = None # сначала присваиваем None, потом вызываем функцию
        # изначально человек не имеет никакой должности Инициализируется, чтобы в методе assign
        # проверять, не привоена ли ему уже должность (директор фирмы)
        self.pos = None

    @classmethod
    def bind_session(cls, session):
        cls.session = session

    def assign(self, firm_id: Optional[int]=None, pos_id: Optional[int]=None):
        '''
        при инициации нужно присвоить человеку какую-то должность. Делается это через таблицу people_positions
        но из инита People сделать запись в нее нельзя, та как у People  в этот момент еще не определен id
        '''
        # как только человеку исполняется 20 лет, он начинает работать
        y = self.birth_date.year + 20
        anniversary_20 = date(year = y, month=self.birth_date.month, day=self.birth_date.day)
        if anniversary_20 <= get_anno():
            self.start_work =  anniversary_20
        self.pos = Position(self, UNEMPLOYED)
        # self.change_position_record()
        self.current_firm_id = UNEMPLOYED
        # self.migrate_record()

    def assign_to_firm(self, firm_id):
        if firm_id:
            self.self.current_firm_id = firm_id
        else:
            self.current_firm_id = Firm.get_rand_firm_id()
        self.migrate_record()
        self.pos.become_worker()  # повышаем с безработного жо первой ступени работника
        self.change_position_record()

    def unemployed_to_worker(self, firm_id=None):
        if self.start_work:
            # трудоспособных безработных надо трудоустроить
            # записи о директорах уже сделаны ранее, теперь надо занести работяг
            if self.current_firm_id == UNEMPLOYED:
                self.assign_to_firm(firm_id)
        else: #  осталась молодежь - записываем, что она не работает
            self.migrate_record()
            self.change_position_record()

    def check_start_work(self, firm_id=None):
        if self.start_work is None:
            # как только человеку исполняется 20 лет, он начинает работать
            if self.age < 20:
                return False
            else:
                # если человеку больше 19 лет иначе он не работает
                self.start_work = get_anno()
                self.assign_to_firm(firm_id)
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
                print(f'{get_anno()} id: {self.id:3d} age: {self.age:3d} Retired')
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
                print(f'{get_anno()} id: {self.id:3d} age: {self.age:3d} Dead')
                self.set_dead()
                return True
            else:
                return False

    def director_retired(self):
        print(f'старый директор id {self.id:3d} ушел из фирмы {self.current_firm_id:3d}')
        for f in People.obj_firms:
            if f.id == self.current_firm_id:
                f.director = None
                break

    def set_retired(self):
        if self.pos.position == Position.CAP:
            self.director_retired()
        self.current_firm_id = UNEMPLOYED
        self.pos.set_position(UNEMPLOYED)
        self.retire_date = get_anno()
        self.migrate_record()
        self.change_position_record()


    def set_dead(self):
        self.death_date = get_anno()
        if self.retire_date is None:
            self.set_retired()

    @property
    def age(self):
        today = get_anno()
        age = (today.year - self.birth_date.year -
               ((today.month, today.day) < (self.birth_date.month, self.birth_date.day))
               )
        return age

    @property
    def experience(self):
        return (get_anno() - self.start_work).days

    def update(self):
        if self.check_death() is False:
            if self.check_retirement() is False:
                if self.check_start_work():
                    if self.current_firm_id !=1: # если не безработный, можно повысить
                        promoted = self.pos.promotion(self.talent, self.experience)
                        if promoted:
                            self.change_position_record()
                    tranfered = self.migrate()
                    if tranfered:
                        self.migrate_record()

    def change_position_record(self):
        self.last_position_id = self.pos.position
        People.session.add(PeoplePosition(
            people_id=self.id,
            position_id=self.pos.position,
            move_to_position_date=get_anno()))

    def migrate_record(self):
        People.session.add(PeopleFirm(
            people_id=self.id,
            firm_id=self.current_firm_id,
            move_to_firm_date=get_anno()))

    def migrate(self, firm_id=None):
        '''
        Переходим в другую фирму
        '''
        if firm_id is not None: # принудительный переход в конкретную фирму
            self.current_firm_id = firm_id
            return True
        else:
            # директору не стоит уходить из своей фирмы
            # если отсутствие работы тоже считать фирмой, то директор даже уволиться не может
            if self.pos.position < Position.CAP:
                targ = Firm.get_rand_firm_id()
                if self.current_firm_id != targ:
                    targ_firm_rating = People.session.query(Firm.last_rating).filter(Firm.id == targ).scalar()
                    attraction_mod = targ_firm_rating - self.recent_firm.last_rating
                    chanse = (40 + attraction_mod) / (40 * 365)
                    if random() < chanse:
                        self.current_firm_id = targ
                        return True
            return False

    def __repr__(self):
        s = f'id: {self.id} {self.last_name} {self.first_name} {self.second_name}, {self.birth_date},' \
            f' талант:{self.talent} фирма: "{self.recent_firm.firmname.name}' \
            f'" долж: "{self.position_name.name}"  нач. работы: {self.start_work}'
        return s