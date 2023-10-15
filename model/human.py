from typing import Optional

from datetime import timedelta, date
from random import choice, randint, random, gauss, sample

from sqlalchemy import Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import relationship

from settings import (get_birthday,
                      get_anno,
                      POSITION_CAP,
                      TRAIT_MAX,
                      UNEMPLOYED_POSITION,
                      firm_creat_probability)

import settings


from model import Base, session
from model.status import NStatus
from model.worker_base import (
                               Position,
                               PeopleFirm,
                               )
from model.firm import Firm



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
    current_status_id =  Column(Integer, ForeignKey('statuses.id'), default= None,  index=True)

    current_position_id = Column(Integer, ForeignKey('positions.id'), index=True)
    death_date = Column(Date, index=True)
    retire_date = Column(Date, index=True)

    recent_firm = relationship('Firm', back_populates='current_emploees')
    # у человека всегда есть позиция, поэтому LEFT OUTER JOIN не требуется
    position_name = relationship('PositionNames',  uselist = False, innerjoin=True)
    worked_in_firms = relationship('PeopleFirm', back_populates='human_conn')

    obj_firms = {}  # доступ к объектам фирм через их id

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
        # изначально человек не имеет никакой должности. Инициализируется, чтобы в методе assign
        # проверять, не присвоена ли ему уже должность (директор фирмы).
        self.status = None
        self.pos = None # позиция, то есть должность в конкретной фирме. Или безработный( = 1)
        self.unemp_counter = 0
        self.ill_counter = 0


    def assign(self):
        '''
        При инициации нужно присвоить человеку какую-то должность. Делается это через таблицу people_positions,
        но из инита People сделать запись в нее нельзя, та как у People в этот момент еще не определен id
        поэтому используется дополнительная процедура инициализации, когда id будет определен.
        Здесь человек ни к какой фирме не приписывается. Он либо молодой, либо безработный
        '''
        # self.status = StatHandle(self) # обработчик статуса. Для доступа к статусу: human.status.status
        self.status = NStatus(self)  # обработчик статуса. Для доступа к статусу: human.status.status
        # как только человеку исполняется 20 лет, он начинает работать
        y = self.birth_date.year + 20
        anniversary_20 = date(year = y, month=self.birth_date.month, day=self.birth_date.day)
        # если на момент начала симуляции, человеу 20 лет или больше, в качестве даты начала работы присваивается дата его двадцатилетия
        if anniversary_20 <= get_anno():
            self.start_work =  anniversary_20
        self.status.live()
        self.pos = Position(self, UNEMPLOYED_POSITION)
        self.pred_firm_id = None
        self.current_firm_id = None

    def aboard(self):
        firms_qty = len(Firm.operating_firms_list())
        if not self.start_work:
            self.start_work = get_anno()
        if firm_creat_probability(firms_qty):
            firm_id = self.create_firm()
            self.migrate(firm_id)
        else:
            self.migrate()
            self.pos.set_position_employed()

    def migrate(self, firm_id=None):
        '''
        Переходим в другую фирму
        '''
        if firm_id is not None:  # принудительный переход в конкретную фирму
            self.set_current_firm_id(firm_id)
            # здесь нужно проверить, есть ли у фирмы директор, а то как-то странно получается,
            # что сразу директора назначаем
            People.obj_firms[self.current_firm_id].assign_new_director()
        else:
            pool_ids = Firm.firm_to_migrate_ids(self.current_firm_id)
            sample_len = max(len(pool_ids), 1)
            sample_len = min(sample_len, 3)  # рассматривается от 2 до 3 фирм для ухода
            pool_ids = sample(pool_ids, k=sample_len)
            assert len(pool_ids), 'Пустой список фирм, в одну из которых человек хочет перевестись'
            # сортировка по рейтингу пока приводит к тому что все идут в одну фирму
            migr_firm_id = session.query(Firm).filter(Firm.id.in_(pool_ids)).order_by(Firm.last_rating.desc()).all()
            "Варианты фирм для миграции"
            for f in migr_firm_id:
                print("фирма", f.id, "рейтинг", f.last_rating)
            migr_firm_id.sort(key=lambda x: x.last_rating, reverse=True)
            best_rating_firm = migr_firm_id[0]
            self.set_current_firm_id(best_rating_firm.id)
            print(f'Личный рейтинг человека: {self.pos.position*self.talent}')
            print(f'Уходим в фирму {best_rating_firm.id}')
        self.migrate_record()


    def create_firm(self, defined_name_id: int = None) -> int:
        firm_name_id = Firm.get_unused_firmname_id() if defined_name_id == None else defined_name_id
        Firm.mark_firmname_as_used(firm_name_id)
        fi = Firm(firm_name_id)
        session.add(fi)
        session.commit()
        print('создали фирму', fi.id)
        self.__class__.obj_firms[fi.id] = fi
        return fi.id

    def define_work_period(self, firm_id):
        self.days_in_firm = 0
        chisl = float(People.obj_firms[firm_id].last_rating - self.talent * self.pos.position)
        znam = TRAIT_MAX * POSITION_CAP

        wp = max(10, int(365 *
                    (3.0
                    + 2 * chisl/znam
                    - 2 * (self.ambitions / TRAIT_MAX)
                    + gauss(0, .5)
                    ) - self.experience*.01
                    )
                 )
        # self.work_period  = wp
        return wp


    def set_current_firm_id(self, new_id):
        self.pred_firm_id = self.current_firm_id
        self.current_firm_id = new_id

    def set_firm_director_to_none(self):
        print(f'старый директор id {self.id:3d} ушел из фирмы {self.current_firm_id:3d}')
        People.obj_firms[self.current_firm_id].director = None


    def set_unemployed(self):
        firm = self.__class__.obj_firms[self.current_firm_id]
        position = self.pos.position
        if self.pos.position == POSITION_CAP:
            self.set_firm_director_to_none()
        self.set_assigned_firm_to_none()
        self.pos.set_position_unemployed()
        firm.check_close()
        if position == POSITION_CAP:
            firm.check_assign_director()

    def set_retired(self):
        print(f'{get_anno()} id: {self.id:3d} age: {self.age:3d} Retired')
        self.retire_date = get_anno()
        self.set_unemployed()


    def set_dead(self):
        print(f'{get_anno()} id: {self.id:3d} age: {self.age:3d} Dead')
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
        return (get_anno() - self.start_work).days if self.start_work else 0



    def set_assigned_firm_to_none(self):
        self.set_current_firm_id(None)
        self.migrate_record()

    def update(self):
        self.status.live()
        # session.commit()
        session.flush()

    def migrate_record(self):
        session.add(PeopleFirm(
            people_id=self.id,
            firm_from_id=self.pred_firm_id,
            firm_to_id=self.current_firm_id,
            move_to_firm_date=get_anno()))




    def verbose_repr(self):
        s = f'id: {self.id} {self.last_name} {self.first_name} {self.second_name}, {self.birth_date},' \
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

    def __repr__(self):
        s = "People < "
        s += f'id: {self.id} {self.last_name} {self.first_name} {self.second_name}, {self.birth_date},'\
        f' талант:{self.talent}'
        s += " >"
        return s