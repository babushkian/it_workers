from random import choice, randint

from sqlalchemy import Column, Integer, ForeignKey, Date, select
from sqlalchemy.orm import relationship

import statistics
from model import Base, session
from model.worker_base import FirmName, FirmRating
from settings import get_anno, POSITION_CAP


class Firm(Base):
    session = None
    __tablename__ = 'firms'
    id = Column(Integer, primary_key=True)
    firmname_id = Column(Integer, ForeignKey('firmnames.id'))
    last_rating = Column(Integer)
    open_date = Column(Date)
    close_date = Column(Date)
    ratings = relationship('FirmRating', backref='firms')
    firmname = relationship('FirmName', back_populates="firm_with_name", uselist=False, innerjoin=True)
    current_emploees = relationship('People', back_populates='recent_firm')
    people = relationship('PeopleFirm', back_populates='firm_conn')

    obj_people = None

    def __init__(self, n):
        self.firmname_id = n
        self.last_rating = 24
        self.open_date = get_anno()
        self.director = None
        self.rating_multiplier = 1.0

    def personal(self) -> list['People']:
        return [i for i in Firm.obj_people if i.current_firm_id == self.id]

    def check_close(self) -> bool:
        if len(self.personal()) == 0:
            self.close_firm()# закрываем фирму, если в ней никого не осталось
            return True
        return False

    def check_assign_director(self):
        if self.close_date is None:  # если ушел директор, назначаем нового
            self.assign_new_director()


    def assign_new_director(self):
        candidates = self.personal()
        # сортируем по способностям - кого лучше назначить
        candidates.sort(key=lambda x: 2 * x.pos.position + x.talent, reverse=True)
        print('========================')
        print(f'в фриме {self.id} {self.firmname.name} смена руководства')
        print(f'Всего сотрудников в фирме: {len(candidates)}')
        print('Кандидаты на пост директора:')
        for i in candidates:
            print(f'id: {i.id:3d} position:{i.pos.position:3d} talent: {i.talent:3d}')
        self.director = candidates[0]
        self.director.pos.set_position_director()
        print(f'Новый директор: {self.director}')


    @staticmethod
    def operating_firms_list()->list[int]:
        '''
        возвращает список id фирм, у которых не проставлена дата закрытия, то есть работающих фирм
        '''
        q = select(Firm.id).filter(Firm.close_date.is_(None))
        res = session.scalars(q).all()
        return res

    @staticmethod
    def get_rand_firm_id()->int:
        '''
        Возвращает случайный идентификатор работающей(не закрытой) фирмы
        '''
        return choice(Firm.operating_firms_list())

    @staticmethod
    def get_used_firmname_ids_pool()->list[int]:
        '''
        Возвращает список идетнтификаторов имен фирм, которые уже заняты существующими фирмами
        '''
        pool = session.query(FirmName.id).filter(FirmName.used==True).all()
        return pool

    @staticmethod
    def get_unused_firmname_id() -> int:
        '''
        Возвращает случайный идентификатор имени фирмы из списка незанятых
        '''
        q = select(FirmName.id).where(FirmName.used == False)
        pool = session.scalars(q).all()
        assert len(pool) > 0, 'не осталось свободных названий фирм'
        new_id = choice(pool)
        return new_id


    @staticmethod
    def firm_to_migrate_ids(firm_id)->list[int]:
        pool = Firm.operating_firms_list()
        if firm_id in pool:
            pool.remove(firm_id)
        return pool

    @staticmethod
    def mark_firmname_as_used( new_id):
        '''
        В таблицу firmname добавляется отметка, что такое имя фирмы занято после создания новой фирмы
        '''
        session.query(FirmName).filter(FirmName.id == new_id).update({'used':True})



    def  new_rating(self):
        '''
        Новое значение рейтинга фирмы генерится случайно
        '''
        self.last_rating = self.director.talent * POSITION_CAP
        session.add(FirmRating(firm_id=self.id, rating=self.last_rating, workers_count=1, rate_date=get_anno()))

    def close_firm(self):
        # !!!! А еще нужно принудительно переключить статус всех работников в безработные
        print(f'Фирма {self.firmname.name} закрылась')
        for i in Firm.obj_people:
            if i.current_firm_id == self.id:
                i.set_assigned_firm_to_none()
        self.close_date = get_anno()


    def update_rating(self):
        workers_rating = [i.pos.position*i.talent for i in Firm.obj_people if i.current_firm_id == self.id]
        if self.rating_multiplier > 0:
            self.rating_multiplier *= 0.8
        if self.rating_multiplier < .05:
            self.rating_multiplier = 0
        self.last_rating  = statistics.mean(workers_rating)* (1 + self.rating_multiplier)
        session.add(FirmRating(firm_id=self.id, rating=self.last_rating, rate_date=get_anno()))

    def __repr__(self):
        return f'<id:{self.id} "{self.firmname.name}"  рейтинг: {self.last_rating}>'