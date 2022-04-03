import os
from sqlalchemy.engine import Engine
from sqlalchemy import Column, Integer, String, create_engine, Date, Time, DateTime, ForeignKey, event, Index
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func, asc, desc, distinct
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

from random import choice, randint, random
from datetime import date, timedelta
from dataclasses import dataclass
from typing import Optional, List, Dict, TypeVar, Generic

cit = ['Омскшина', 'Новотэк', 'Карсонс', 'Иркасофт', 'Томат', 'Тюмция', 'Курганстрой', 'Хабр', 
    'Барнс', 'Норштокинг', 'Пенестер', 'Костромарс']

first = ['Алексей', 'Александр', 'Николай', 'Дмитрий', 'Сергей', 'Иван', 'Юрий', 'Артем', 'Антон', 'Кирилл', 'Валерий']
second = ['Алексеевич', 'Александрович', 'Николаевич', 'Дмитриевич', 'Сергеевич', 'Иванович', 'Юрьевич', 'Артемович', 
'Антонович', 'Кириллович', 'Валерьевич']
last = ['Иванов', 'Петров', 'Сидоров', 'Ковалев', 'Кузнецов', 'Соколов', 'Потапов', 'Савченко', 'Петухов', 'Горяев',
'Васильев', 'Давыдов', 'Воробьев', 'Орлов', 'Уткин', 'Трамваев', 'Задорожный', 'Королев', 'Самойлов', 'Серов', 'Шишкин', 
'Медведев', 'Гусев', 'Волков', 'Густопсов', 'Хомяков','Чайкин', 'Чаплин', 'Кабанов', 'Голиков', 'Густоперов','Говоров', 
'Жеребилов', 'Покровский','Алишеров', 'Донской', 'Черданцев', 'Барановский', 'Чепига', 'Парахуда', 'Пережога']

# количество фирм
COL_FIRM = len(cit)

# возвращяет индекс случайной фирмы
def get_rand_firm_id()->int:
    return(randint(1, COL_FIRM))

#
ANNO = date(2010, 1, 1)

@dataclass
class MoveRecord:
    from_id: int
    to_id: int
    move_date: date

TALENT_MIN = -3
TALENT_MAX = 3
TALENT_RANGE = TALENT_MAX - TALENT_MIN
EXPERIENCE_CAP = 1/(365*40) # через сорок лет работы шанс получить повышение удваивается



class Position():
    MULTIPLIER = 365
    PROGRESSION = 2 # основание степени (тружность получения повышения растет по степенному закону в завистмости от должности)
    POSITIONS = ['стажёр', 'инженер', 'старший инженер', 'главный инженер', 'начальник отдела', 'начальник департамента', 'директор']
    CAP = len(POSITIONS) - 1

    def __init__(self, ses):
        self.session = ses
        self.__position = self.session.query(PosBase.id).first()[0]


    @property
    def position(self):
        return self.__position

    @property
    def posname(self):
        pos = self.session.query(PosBase.name).filter(PosBase.id == self.__position).scalar()
        return pos

    # повышение по службе
    def promotion(self, talent, experience):
        if self.__position < Position.CAP:
            x = random()
            base_mod = 1/(Position.MULTIPLIER * Position.PROGRESSION**self.__position)
            # умножает время перехода на следующую должность. Для умных время до повыщения сокращается
            chisl = (2*TALENT_MAX  + talent)
            talent_mod =chisl/TALENT_RANGE
            experience_mod = (1 + EXPERIENCE_CAP * experience)
            if x < base_mod * talent_mod * experience_mod:
                self.__position += 1
                return True
            return False

    def demotion():# понижние по службе
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
    __table_args__ = (Index('ix_human_firms_human_id_firm_id',human_id, firm_id), )


class HumanPosition(Base):
    __tablename__ = 'human_positions'
    id = Column(Integer, primary_key=True)
    human_id = Column(Integer, ForeignKey('humans.id'))
    pos_id = Column(Integer, ForeignKey('positions.id'))
    move_to_position_date = Column(Date, index=True)
    __table_args__ = (Index('ix_human_positions_human_id_pos_id',human_id, pos_id), )

class Human(Base):
    __tablename__ = 'humans'
    id = Column(Integer, primary_key=True)
    fname = Column(String(50))
    sname = Column(String(50))
    lname = Column(String(50))
    age = Column(Date)
    talent = Column(Integer)
    srart_work = Column(Date)
    firm_id = Column(Integer, ForeignKey('firms.id'))
    pos_id = Column(Integer, ForeignKey('positions.id'))

    #дата начала работы
    # стажю. От него зависит вероятность продвижения по карьернойц лестнице
    # карьерная лестница: несколько ступеней, вероятность продвиджения на следуюшую ступень меньше, чем на предыдущую (*1.5)
    # талант: влияет на веротность повышения и на вероятность понижения
    HUM_COUNTER = 1
    def __init__(self, ses, firm_dict, firm_id):
        self.firm_dict = firm_dict
        self.session = ses
        self.fname = choice(first) #
        self.sname = choice(second) #
        self.lname = choice(last) #
        self.age = date(randint(1960, 1989), randint(1, 12), randint(1, 28)) # день рождения
        self.talent = randint(TALENT_MIN, TALENT_MAX)
        self.srart_work = self.age + timedelta(days=365*20)  # дата начала работы
        self.pos = Position(self.session)
        self.pos_id = self.pos.position
        self.firm = self.firm_dict[firm_id]
        self.firm_id = firm_id

    @property
    def position(self):
        return self.pos.position

    @property
    def experience(self):
        return (ANNO - self.srart_work).days

    def update(self):
        promoted = self.pos.promotion(self.talent, self.experience)
        if promoted:
            self.session.add(HumanPosition(human_id=self.id, pos_id = self.pos.position, move_to_position_date=ANNO))
            self.pos_id = self.pos.position
        tranfered = self.migrate()
        if tranfered:
            self.session.add(HumanFirm(human_id=self.id, firm_id =self.firm_id, move_to_firm_date=ANNO))

    def migrate(self):
        targ = get_rand_firm_id()
        if self.firm_id != targ:
            attraction_mod = self.firm_dict[targ].attraction - self.firm.attraction 
            chanse = (40 + attraction_mod)/(40*365)
            if random() < chanse:
                self.firm = self.firm_dict[targ]
                self.firm_id = self.firm.id
                return True
        return False

    def __repr__(self):
        s = f'{self.lname} {self.fname} {self.sname}, {self.age}, талант:{self.talent} \
        фирма: "{self.firm.name}" долж:{self.pos.posname}, стаж: {self.experience}'
        return s


class Firm(Base):
    __tablename__ = 'firms'
    id = Column(Integer, primary_key=True)
    name = Column(String(70))
    
    def __init__(self,  name):
        self.name = name
        self.attraction = randint(10, 40)

    def populate(self, num):
        for _ in range(num):
            self.residents.append(None) 

    def update(self):
        if ANNO.day==1 and ANNO.month==1:
            self.attraction += randint(-4, 4)

    def __repr__(self):
        return f'<id:{self.id} "{self.name}"  престижность: {self.attraction}>'



@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode = MEMORY")
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.close()

# engine = create_engine(f"sqlite:///workers.db", echo=True)
engine = create_engine(f"sqlite:///workers.db", echo=False)

# удаляем все таблицы, чтобы введенные прежде данные не мешали
Base.metadata.drop_all(engine)

Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

# создаем все таблицы
Base.metadata.create_all(engine)

firms_list  = list()
for  name in cit:
    fi = Firm(name)
    firms_list.append(fi)

session.add_all(firms_list)
session.commit()
firm_dict = {}
for i in firms_list:
    firm_dict[i.id] = i


for i in Position.POSITIONS:
    session.add(PosBase(name = i))
session.commit()    


people = list()
for i in range(400):
    people.append(Human(session, firm_dict,  get_rand_firm_id()))
session.add_all(people)
session.commit()

for t in range(365*5):
    ANNO += timedelta(days=1)
    for f in firms_list:
        f.update()
    for p in people:
        p.update()
    session.commit()

# ---------------------------------------------
# конец симуляции
# дальше будут статистические вычисления

poo = {i: 0 for i in range(len(Position.POSITIONS))}
print(ANNO)
for p in people:
    poo[p.pos.position] +=1

for i in poo:
    print(Position.POSITIONS[i], poo[i])

print('='*20)

talcount = {i: 0 for i in range(TALENT_MIN, TALENT_MAX+1)}
talsum = {i: 0 for i in range(TALENT_MIN, TALENT_MAX+1)}
for p in people:
    talcount[p.talent] +=1
    talsum[p.talent] +=p.pos.position


print('Среднее значение должности в зависмомти от таланта')
talmean = dict()
for c in talcount:
    if talcount[c] == 0:
        talmean[c] = 0
    else:
        talmean[c] = talsum[c]/talcount[c]
    print(f'талант:{c}, людей {talcount[c]} средняя должность {talmean[c]}')


limits = {0:20, 1:30, 2:40, 3:50, 4:60, 5:1000}

def pockets(birth):
    ret = None
    age = (ANNO - birth).days/365
    for i in limits:
        if age < limits[i]:
            ret = i
            break
    return i


agcount = {i: 0 for i in range(len(limits))}
agsum = {i: 0 for i in range(len(limits))}
for p in people:
    agcount[pockets(p.age)] +=1
    agsum[pockets(p.age)] +=p.pos.position

print('='*20)
print('Среднее значение должности в зависмомти от возраста')
agmean = dict()
for c in agcount:
    if agcount[c] == 0:
        agmean[c] = 0
    else:
        agmean[c] = agsum[c]/agcount[c]
    print(f'возраст до:{limits[c]}, людей {agcount[c]} средняя должность {agmean[c]}')

# вычисляем количество людей в каждой фирме
firms_count = {i:0 for i in firms_list}
for p in people:
    firms_count[p.firm] +=1

print('='*20)
for f in firms_count:
    print(f'{f.name}  рпестиж: {f.attraction}   сотрудников: {firms_count[f]}')

# просто выборка из несеольких людей
print('='*20)
for i in people[:16]:
    print(i)

# выборка имеющихся должностей
print('='*20)
psd = session.query(distinct(Human.pos_id)).order_by(Human.pos_id).all()
print(psd)

# РАСПРЕДЕЛЕНИЕ ЛЮДЕЙ ПО ДОЛЖНОСТЯМ
# первый способ
print('*'*40, '/nпервый способ')
x = session.query(func.count(Human.id).label('cont'), PosBase.name).join(PosBase).group_by(Human.pos_id).order_by(Human.pos_id).all()
for y in x:
    print(y.cont, y.name)

# второй способ
print('*'*40, '/nвторой способ')
for i in psd:
    x = session.query(func.count(Human.id))
    x = x.filter(Human.pos_id == i[0])
    x =x.scalar()
    print(session.query(PosBase.name).filter(PosBase.id == i[0]).scalar(), x)