import os
from sqlalchemy import Column, Integer, String, create_engine, Date, Time, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func, asc, desc
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

COL_FIRM = len(cit)

def get_ranf_firm_id()->int:
    return(randint(1,COL_FIRM))


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
    PROGRESSION = 2


    POSITIONS = ['стажёр', 'инженер', 'старший инженер', 'главный инженер', 'начальник отдела', 'начальник департамента', 'директор']
    CAP = len(POSITIONS) - 1

    def __init__(self, pos = 0):
        self.__position = pos

    @property
    def position(self):
        return self.__position

    @property
    def posname(self):
        return Position.POSITIONS[self.__position]


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

    def demotion(self):# понижние по службе
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
    move_to_firm_date = Column(Date)

class HumanPosition(Base):
    __tablename__ = 'human_positions'
    id = Column(Integer, primary_key=True)
    human_id = Column(Integer, ForeignKey('humans.id'))
    pos_id = Column(Integer, ForeignKey('positions.id'))
    move_to_position_date = Column(Date)



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

    #дата начала работы
    # стажю. От него зависит вероятность продвижения по карьернойц лестнице
    # карьерная лестница: несколько ступеней, вероятность продвиджения на следуюшую ступень меньше, чем на предыдущую (*1.5)
    # талант: влияет на веротность повышения и на вероятность понижения
    HUM_COUNTER = 1
    def __init__(self, firm_id):
        self.fname = choice(first) #
        self.sname = choice(second) #
        self.lname = choice(last) #
        self.age = date(randint(1960, 1989), randint(1, 12), randint(1, 28)) # день рождения
        self.talent = randint(TALENT_MIN, TALENT_MAX)
        self.srart_work = self.age + timedelta(days=365*20)  # дата начала работы
        self.pos = Position()
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
            pass # записать событие о повышении в базу данных
        tranfered = self.migrate()
        if tranfered:
            pass # запись в базу о том, что человек перешел в другую фирму

    def migrate(self):
        targ = get_ranf_firm_id()
        if self.firm_id != targ:
            attraction_mod = targ.attraction - self.firm.attraction 
            chanse = (40 + attraction_mod)/(40*365)
            if random() < chanse:
                self.firm = targ
                return True
        return False


    def __repr__(self):

        s = f'{self.lname} {self.fname} {self.sname}, {self.age}, талант:{self.talent} фирма: "{self.firm.name}" долж:{self.pos.posname}, стаж: {self.experience}'
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



engine = create_engine(f"sqlite:///workers.db", echo=True)
Base.metadata.drop_all(engine)
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

Base.metadata.create_all(engine)


firms_list  = list()
for  name in cit:
    fi = Firm(name)
    firms_list.append(fi)

session.add_all(firms_list)
session.commit()

for i in Position.POSITIONS:
    session.add(PosBase(name = i))
session.commit()    


people = list()
for i in range(300):
    people.append(Human(get_ranf_firm_id()))
session.add_all(people)
session.commit()

for t in range(365*4):
    ANNO += timedelta(days=1)
    for f in firms_list:
        f.update()
    for p in people:
        p.update()

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

print('Среднее значение должности в зависмомти от возраста')
agmean = dict()
for c in agcount:
    if agcount[c] == 0:
        agmean[c] = 0
    else:
        agmean[c] = agsum[c]/agcount[c]
    print(f'возраст до:{limits[c]}, людей {agcount[c]} средняя должность {agmean[c]}')

firms_count = {i:0 for i in firms_list}
for p in people:
    firms_count[p.firm] +=1

for f in firms_count:
    print(f'{f.name}  рпестиж: {f.attraction}   сотрудников: {firms_count[f]}')


for i in people[:16]:
    print(i)