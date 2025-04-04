from random import random, randint, gauss
from math import ceil
from enum import Enum
from sqlalchemy import Column, Integer, String,  Date, Boolean,  ForeignKey, Index
from sqlalchemy.orm import relationship
from transitions import Machine

from model import Base, session

from settings import (get_anno, DEATH_MIN_AGE, DEATH_DELTA,
                      RETIREMENT_MIN_AGE,RETIREMENT_DELTA,
                      ILL_BASE_CHANCE,
                      VACATION_CHANCE,
                      YEAR_LENGTH,
                      firm_creat_probability,
                      POSITION_CAP
                      )

from model.firm import Firm

class Status(int, Enum):
    UNKNOWN = 1
    YOUNG = 2
    UNEMPLOYED = 3
    EMPLOYED = 4
    VACATION = 5
    ILL = 6
    RETIRED = 7
    DEAD = 8

statnames = {
    Status.UNKNOWN: 'неопределенный',
    Status.YOUNG: 'молод ещё',
    Status.UNEMPLOYED: 'безработный',
    Status.EMPLOYED: 'работает',
    Status.VACATION: 'в отпуске',
    Status.ILL: 'на больничном',
    Status.RETIRED: 'на пенсии',
    Status.DEAD: 'умер',
}

class StatusName(Base):
    '''
    таблица содержит перечисление разных статусов человека: безработный, работает, пенсионер, умер
    фактически нужна для извлечения названий статусов человека
    '''
    __tablename__ = 'statuses'
    id = Column(Integer, primary_key=True)
    name = Column(String(200))

class PeopleStatus(Base):
    '''
    состояния в которых находятся люди: молод для работы, работает, пенсионер
    имеется калонка "дата" - когда человек перешел в указанное состояние
    '''
    __tablename__ = 'people_status'
    id = Column(Integer, primary_key=True)
    people_id = Column(Integer, ForeignKey('people.id'))
    status_id = Column(Integer, ForeignKey('statuses.id'))
    status_date = Column(Date, index=True)
    __table_args__ = (Index('ix_people_status_people_id_status_id', people_id, status_id),)
    status_name = relationship('StatusName')



class NStatus:

    def __init__(self, hum: 'People'):
        self.human: 'People' = hum

        self.work_time = 0
        self.idle_time = 0
        self.ill_time = 0
        self.vacation_time = 0
        self.working_range = 0
        self.idle_range = 0
        self.ill_range = 0
        self.vacation_range = 0
        self.dead = False
        self.retired = False
        self.machine = Machine(model=self,
                               states=Status,
                               initial=Status.UNKNOWN,
                               auto_transitions=False,
                               after_state_change=self.status_record)
        self.machine.add_transition(trigger='die',
                                    source=[Status.EMPLOYED, Status.UNEMPLOYED, Status.RETIRED],
                                    dest=Status.DEAD,
                                    conditions=self.check_death,
                                    after=self.human.set_dead)
        self.machine.add_transition(trigger='retire',
                                    source=[Status.UNEMPLOYED, Status.EMPLOYED],
                                    dest=Status.RETIRED,
                                    conditions=self.check_retirement,
                                    after=self.human.set_retired)
        self.machine.add_transition(trigger='get_old',
                                    source=[Status.YOUNG, Status.UNKNOWN],
                                    dest=Status.UNEMPLOYED,
                                    conditions=self.check_adult)
        self.machine.add_transition(trigger='get_young',
                                    source=Status.UNKNOWN,
                                    dest=Status.YOUNG)
        self.machine.add_transition(trigger='get_fired',
                                    source=Status.EMPLOYED,
                                    dest=Status.UNEMPLOYED,
                                    conditions=self.check_unemployed,
                                    prepare=self.employed_prepare,
                                    after=[self.human.set_unemployed, self.get_fired_after])
        self.machine.add_transition(trigger='get_job',
                                    source=Status.UNEMPLOYED,
                                    dest=Status.EMPLOYED,
                                    conditions=self.check_employed,
                                    prepare=self.unemployed_prepare,
                                    after=self.get_job_after)
        self.machine.add_transition(trigger='get_ill',
                                    source=Status.EMPLOYED,
                                    dest=Status.ILL,
                                    conditions=self.check_ill)
        self.machine.add_transition(trigger='get_vacation',
                                    source=Status.EMPLOYED,
                                    dest=Status.VACATION,
                                    conditions=self.check_vacation)
        self.machine.add_transition(trigger='back_to_work',
                                    source=[Status.ILL, Status.VACATION],
                                    dest=Status.EMPLOYED,
                                    conditions=self.check_back_to_work,
                                    prepare=self.back_to_work_prepare,
                                    after=self.back_to_work_after)

    def status_record(self):
        self.human.current_status_id = self.state.value
        session.add(PeopleStatus(people_id=self.human.id,
                                 status_id=self.state.value,
                                 status_date=get_anno()))
        session.commit()

    def employed_prepare(self):
        # print('Плюс год работы!!!')
        self.work_time += 1

    def unemployed_prepare(self):
        # print('Тусуюсь без работы!!!')
        self.idle_time += 1

    def back_to_work_prepare(self):
        if self.ill_range > 0:
            # print("Болею")
            self.ill_time += 1
        else:
            # print("В отпуске")
            self.vacation_time += 1


    def get_job_after(self):
        self.human.aboard()
        self.working_range = self.human.define_work_period(self.human.current_firm_id)
        print(f'{self.human} собираюсь работать {self.working_range} дней')
        self.idle_time = 0


    def get_fired_after(self):
        self.idle_range = ceil(abs(gauss(0, 1.5)))*7
        print(f'Буду бездельничать {self.idle_range} дней')
        self.work_time = 0

    def get_ill_after(self):
        print("Я заболел")

    def get_vacation_after(self):
        print("Я пошел в отпуск")

    def back_to_work_after(self):
        self.ill_range = 0
        self.ill_time = 0
        self.vacation_range = 0
        self.vacation_time = 0


    def check_adult(self):
        return self.human.age > 19

    def check_unemployed(self):
        return self.work_time >= self.working_range


    def check_employed(self):
        return self.idle_time >= self.idle_range

    def check_ill(self):
        if self.ill_range == 0 and random() < ILL_BASE_CHANCE/self.human.health:
            self.ill_range = ceil(abs(gauss()) * 7)  # на сколько недель больничный
            print(f'Болезнь на {self.ill_range} ходов')
        return self.ill_range != 0

    def check_vacation(self):
        if self.vacation_range == 0 and random() < VACATION_CHANCE:
            self.vacation_range = 28
            print(f'Отпуск на {self.vacation_range} ходов')
        return self.vacation_range != 0

    def check_back_to_work(self):
        ill = (self.ill_range > 0) and (self.ill_time >= self.ill_range)
        vac = (self.vacation_range > 0) and (self.vacation_time >= self.vacation_range)
        return ill or vac

    def check_retirement(self):
        if self.human.age >= RETIREMENT_MIN_AGE:
            treshold = (self.human.age + 1 - RETIREMENT_MIN_AGE)/(RETIREMENT_DELTA*YEAR_LENGTH)
            if random() < treshold:
                self.retired = True
        return self.retired

    def check_death(self):
        if self.human.age >= DEATH_MIN_AGE:
            treshold = (self.human.age - DEATH_MIN_AGE)/(18*DEATH_DELTA*YEAR_LENGTH)
            if random() < treshold:
                self.dead = True
        return self.dead

    def live(self):
        if self.state != Status.DEAD:
            if self.state == Status.EMPLOYED:
                self.human.pos.promotion_attempt()


            triggers = self.machine.get_triggers(self.state)
            for t in triggers:
                mt = 'may_' + t
                test_trans = getattr(self, mt)()
                # print(f'{t}: {test_trans}')
                if test_trans:
                    ex = getattr(self, t)
                    result = ex()
                    print("человек", self.human.id, 'принял состояние', t, self.state)
                    break

