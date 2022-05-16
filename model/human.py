from typing import Optional

from datetime import timedelta, date
from random import choice, randint, random, gauss, sample

from sqlalchemy import Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import relationship

from model import Base, session
import settings
from model.status import Status, StatHandle, StatusName, PeopleStatus
from model.worker_base import (
                               Position,
                               PeoplePosition,
                               PeopleFirm,
                               )
from model.firm import Firm
from settings import (get_birthday,
                      get_anno,
                      RETIREMENT_MIN_AGE,
                      RETIREMENT_DELTA,
                      DEATH_DELTA,
                      YEAR_LENGTH,
                      DEATH_MIN_AGE,
                      UNEMPLOYED_POSITION,
                      POSITION_CAP,
                      TRAIT_MAX,)


class People(Base):
    __tablename__ = 'people'
    id = Column(Integer, primary_key=True)
    first_name = Column(String(50))
    second_name = Column(String(50))
    last_name = Column(String(50))
    birth_date = Column(Date, index=True)
    talent = Column(Integer, index=True)
    start_work = Column(Date)
    current_firm_id = Column(Integer, ForeignKey('firms.id'), default= None,  index=True)
    last_position_id = Column(Integer, ForeignKey('positions.id'),  index=True)
    death_date = Column(Date, index=True)
    retire_date = Column(Date, index=True)

    recent_firm = relationship('Firm', back_populates='recent_emploees')
    # у человека всегда есть позиция, поэтому LEFT OUTER JOIN не требуется
    position_name = relationship('PosBase',  uselist = False, innerjoin=True)
    worked_in_firms = relationship('PeopleFirm', back_populates='human_conn')

    obj_firms = None

    # дата начала работы
    # стаж. От него зависит вероятность продвижения по карьерной лестнице
    # карьерная лестница: несколько ступеней, вероятность продвиджения на следующую ступень меньше,
    # чем на предыдущую
    # талант: влияет на вероятность повышения и на вероятность понижения

    def __init__(self):
        self.first_name = choice(settings.first_name)  #
        self.second_name = choice(settings.second_name)  #
        self.last_name = choice(settings.last_name)  #
        self.birth_date = get_birthday()  # день рождения
        self.talent = randint(settings.TRAIT_MIN, settings.TRAIT_MAX)
        self.health = randint(settings.TRAIT_MIN, settings.TRAIT_MAX)
        self.ambitions = randint(settings.TRAIT_MIN, settings.TRAIT_MAX)
        self.start_work = None # сначала присваиваем None, потом вызываем функцию
        # изначально человек не имеет никакой должности Инициализируется, чтобы в методе assign
        # проверять, не привоена ли ему уже должность (директор фирмы)
        self.status = None
        self.pos = None
        self.unemp_counter = 0
        self.ill_counter = 0


    def assign(self):
        '''
        при инициации нужно присвоить человеку какую-то должность. Делается это через таблицу people_positions
        но из инита People сделать запись в нее нельзя, та как у People  в этот момент еще не определен id
        поэтому используется дополнительная процедура инициализации, когда id будет определен.
        Здесь человек ни к какой фирме не приписывается. Он либо молодой либо безработный
        '''
        self.status = StatHandle(self)
        # как только человеку исполняется 20 лет, он начинает работать
        y = self.birth_date.year + 20
        anniversary_20 = date(year = y, month=self.birth_date.month, day=self.birth_date.day)
        # если на момент начала симуляции, человеу 20 лет или больше, в качестве даты начала работы присваивается дата его двадцатилетия
        if anniversary_20 <= get_anno():
            self.start_work =  anniversary_20
            self.status.set_status_unemployed()
        else:
            self.status.set_status_young()
        self.pos = Position(self, UNEMPLOYED_POSITION)
        self.pred_firm_id = None
        self.current_firm_id = None

    def unemployed_to_worker(self, firm_id=None):
        '''
        Вызывается только при инициализации симуляции.
        Третий этап инициализации человека. фирмы уже созданы, директора к ним приписаны.
        Теперь нужно оставшихся людей приписать к фирмам, если они достигли рабочего возраста.
        '''
        if self.status.status == Status.UNEMPLOYED:
            self.assign_to_firm(firm_id)
        else: #  осталась молодежь - просто фиксируем, что она не принадлежит никакой фирме  и имеет статус безработный
            self.migrate_record()
            self.change_position_record()


    def define_work_period(self, firm_id):
        self.days_in_firm = 0
        wp = max(10, int(365 *
                    (3
                    + 2 *(People.obj_firms[firm_id].last_rating - self.talent * self.pos.position) / (TRAIT_MAX * POSITION_CAP)
                    - 2 * (self.ambitions / TRAIT_MAX)
                    + gauss(0, .5)
                    ) - self.experience*.01
                    )
                 )
        self.work_period  = wp
        print(f'{self.work_period=}')

    def assign_to_firm(self, firm_id=None):

        if firm_id:
            self.set_current_firm_id(firm_id)
        else:
            self.set_current_firm_id(Firm.get_rand_firm_id())
        self.define_work_period(self.current_firm_id)
        if self.start_work is None:
            self.start_work = get_anno()
        self.migrate_record()
        self.pos.become_worker()  # повышаем с безработного жо первой ступени работника
        self.status.set_status_employed()
        self.change_position_record()


    def set_current_firm_id(self, new_id):
        self.pred_firm_id = self.current_firm_id
        self.current_firm_id = new_id



    def check_start_work(self):
        # как только человеку исполняется 20 лет, он переходит в ранг безработного а там и работу найдет
        if self.age < 20:
            return False
        else:
            return True


    def check_retirement(self):
        if self.age < RETIREMENT_MIN_AGE:
                return False
        else:
            treshold =  (self.age + 1 - RETIREMENT_MIN_AGE)/(RETIREMENT_DELTA*YEAR_LENGTH)
            if random() < treshold:
                return True
            else:
                return False

    def check_death(self):
        if self.age < DEATH_MIN_AGE: # возраст слишком ранний для умирания
            return False
        else: # Есть возможность умереть
            treshold = (self.age - DEATH_MIN_AGE)/(18*DEATH_DELTA*YEAR_LENGTH)
            if random() < treshold: # повезло, умер
                return True
            else:
                return False

    def director_retired(self):
        print(f'старый директор id {self.id:3d} ушел из фирмы {self.current_firm_id:3d}')
        People.obj_firms[self.current_firm_id].director = None

    def set_retired(self):
        print(f'{get_anno()} id: {self.id:3d} age: {self.age:3d} Retired')
        if self.pos.position == POSITION_CAP:
            self.director_retired()
        self.set_current_firm_id(None)
        self.pos.set_position(UNEMPLOYED_POSITION)
        self.status.set_status_retired()
        self.retire_date = get_anno()
        self.migrate_record()
        self.change_position_record()

    def set_dead(self):
        print(f'{get_anno()} id: {self.id:3d} age: {self.age:3d} Dead')
        self.death_date = get_anno()
        if self.status.status != Status.RETIRED:
            self.set_retired()
        self.status.set_status_dead()


    @property
    def age(self):
        today = get_anno()
        age = (today.year - self.birth_date.year -
               ((today.month, today.day) < (self.birth_date.month, self.birth_date.day))
               )
        return age

    @property
    def experience(self):
        return (get_anno() - self.start_work).days if self.start_work else 0


    def check_illness(self):
        return random() < 1/380

    def check_unemployed(self):
        if self.days_in_firm > self.work_period:
            return random() < 1 / 30
        return False

    def get_illness(self):
        self.ill_counter = int(1 / (0.13 * random() + 0.008) )
        #print(f'{get_anno()} id: {self.id:3d} заболел. срок болезни: {self.ill_counter} дней')
        self.status.set_status_ill()

    def go_to_work(self):
        #print(f'{get_anno()} id: {self.id:3d} вылечился')
        self.status.set_status_employed()

    def get_unemployed(self):
        self.unemp_counter = int(1 / (0.12 * random() + 0.01) )
        print(f'{get_anno()} id: {self.id:3d} уволился. сидеть без работы: {self.unemp_counter} дней')
        self.status.set_status_unemployed()
        self.pred_firm_id = self.current_firm_id
        self.current_firm_id = None
        self.migrate_record()


    def update(self):
        # если мертвый ничего не делаем
        if self.status.status != Status.DEAD:
            if self.check_death():
                self.set_dead()
            if self.status.status != Status.RETIRED:
                if self.check_retirement():
                    self.set_retired()

                if self.status.status ==Status.ILL:
                    self.ill_counter -= 1
                    if self.ill_counter < 1:
                        self.go_to_work()

                elif self.status.status == Status.UNEMPLOYED:
                    self.unemp_counter -= 1
                    if self.unemp_counter < 1:
                        self.assign_to_firm()
                elif self.status.status == Status.EMPLOYED:
                    # сначала смотрим, не заболел ли человек
                    # потом проверяемЮ не уволился ли
                    # если не уволился и не заболел проверяем на повышение и переход в другую фирму
                    self.days_in_firm += 1
                    ill  = self.check_illness()
                    if ill:
                        self.get_illness()
                    unemp = False
                    if not ill:
                        unemp = self.check_unemployed()
                        if unemp:
                            self.get_unemployed()
                    if not (ill or unemp):
                        promoted = self.pos.promotion(self.talent, self.experience)
                        if promoted:
                            self.change_position_record()
                        transfered = self.check_migrate()
                        if transfered:
                            self.migrate()
                            self.migrate_record()


                elif self.status.status == Status.YONG:
                    self.check_start_work()
                    self.get_unemployed()

    def change_position_record(self):
        self.last_position_id = self.pos.position
        session.add(PeoplePosition(
            people_id=self.id,
            position_id=self.pos.position,
            move_to_position_date=get_anno()))

    def migrate_record(self):
        session.add(PeopleFirm(
            people_id=self.id,
            firm_from_id=self.pred_firm_id,
            firm_to_id=self.current_firm_id,
            move_to_firm_date=get_anno()))



    def check_migrate(self):
        if self.days_in_firm > self.work_period:
             return random() <1/10 # должен перейти на другую работу за 10 дней
        return False

    def migrate(self, firm_id=None):
        '''
        Переходим в другую фирму
        '''
        if firm_id is not None: # принудительный переход в конкретную фирму
            self.set_current_firm_id(firm_id)
        else:
            # директору не стоит уходить из своей фирмы
            # если отсутствие работы тоже считать фирмой, то директор даже уволиться не может
            if self.pos.position < POSITION_CAP:
                pool_ids = Firm.firm_to_migrate_ids(self.current_firm_id)
                if len(pool_ids)>3:
                    pool_ids = sample(pool_ids, k=3)
                assert len(pool_ids), 'Пустой список фирм, в одну из которых человек хочет перевестись'
                # изменить запрос на .first()    .all() здесь только для наглядности списка фирм
                migr_firm_id = session.query(Firm).filter(Firm.id.in_(pool_ids)).order_by(Firm.last_rating.desc()).all()
                for  f in migr_firm_id:
                    print(f.id, f.last_rating)
                self.set_current_firm_id(migr_firm_id[0].id)
                print(f'Личный рейтинг: {self.pos.position*self.talent}')
                print(f'Уходим в фирму {migr_firm_id[0].id}')
        self.define_work_period(self.current_firm_id)




    def __repr__(self):
        s = f'id: {self.id} {self.last_name} {self.first_name} {self.second_name}, {self.birth_date},'\
        f' талант:{self.talent}'
        s += f'" долж: "{self.position_name.name}"  нач. работы: {self.start_work}'

        if self.retire_date is not None:
            if self.death_date == self.retire_date:
                s += f' | скорпостижно скончался: {self.death_date}'
            else:
                s += f' | вышел на пенсию: {self.retire_date}'
                if self.death_date is not None:
                    s += f' | скончался: {self.death_date}'
        if self.current_firm_id is not None:
            s += f' | фирма: "{self.recent_firm.firmname.name}"'
        else:
            s += ' | безработный'
        return s