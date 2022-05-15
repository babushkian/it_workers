from random import choice, randint

from sqlalchemy import Column, Integer, ForeignKey, Date
from sqlalchemy.orm import relationship

from model import Base, session
from model.worker_base import FirmName, FirmRating
from settings import get_anno


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
    recent_emploees = relationship('People', back_populates='recent_firm')
    people = relationship('PeopleFirm', back_populates='firm_conn')

    obj_people = None

    def __init__(self, n):
        self.firmname_id = n
        self.last_rating = self.new_rating()
        self.open_date = get_anno()
        self.director = None


    def initial_assign_director(self, director: 'People'):
        self.director = director
        director.migrate(firm_id=self.id)
        director.status.set_status_employed()
        director.migrate_record()
        director.pos.become_director()
        director.change_position_record()

    def assign_new_director(self):
        candidats = [i for i in Firm.obj_people if i.current_firm_id == self.id]
        candidats.sort(key = lambda x: 2*x.pos.position + x.talent, reverse=True)
        print('========================')
        print(f'в фриме {self.id} {self.firmname.name} смена руководства')
        print('Кандидаты на пост директора:')
        for i in candidats:
            print(f'id: {i.id:3d} position:{i.pos.position:3d} talent: {i.talent:3d}')
        self.director = candidats[0]
        print(f'Новый директор: {self.director}')
        self.director.pos.become_director()
        self.director.change_position_record()

    @staticmethod
    def get_rand_firm_id()->int:
        '''
        Возвращает случайный идентификатор работающей(не закрытой) фирмы
        '''
        pool = session.query(Firm.id).filter(Firm.close_date.is_(None)).all()
        return choice(pool)[0]

    @staticmethod
    def get_used_firmname_ids_pool()->list[int]:
        '''
        Возвращает список идетнтификаторов имен фирм, которые уже заняты существующими фирмами
        '''
        pool = session.query(FirmName.id).filter(FirmName.used==True).all()
        return pool

    @staticmethod
    def get_unused_firmname_id()->int:
        '''
        Возвращает случайный идентификатор имени фирмы из списка незанятых
        '''
        pool = session.query(FirmName.id).filter(FirmName.used==False).all()
        assert len(pool) > 0, 'нет свободных названий фирм'
        new_id = choice(pool)[0]
        return new_id

    @staticmethod
    def mark_firmname_as_used( new_id):
        '''
        В таблицу firmname добавляется отметка, что такое имя фирмы занято после создания новой фирмы
        '''
        session.query(FirmName).filter(FirmName.id == new_id).update({'used':True})



    def update(self):
        '''
        В первый день года обновляется рейтинг фирмы
        А так же проверяется, есть ли у фирмы деректор. Если нет, назначается новый
        '''
        if get_anno().day == 1 and get_anno().month == 1:
            self.update_rating()
        if self.director is None:
            self.assign_new_director()

    def  new_rating(self)->int:
        '''
        Новое значение рейтинга фирмы генерится случайно
        '''
        return randint(10, 40)

    def update_rating(self):
        '''
        Изменение рейтинга генерится случайно, и делается запись об изменении рейтинга.
        '''
        r = self.last_rating + randint(-4, 4)
        self.last_rating = max(0, r)
        session.add(FirmRating(firm_id=self.id, rating=self.last_rating, rdate=get_anno()))

    def __repr__(self):
        return f'<id:{self.id} "{self.firmname.name}"  рейтинг: {self.last_rating}>'