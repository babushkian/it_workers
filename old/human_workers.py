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


def return_man(maxcity):
    na = choice(first)
    ot = choice(second)
    fa = choice(last)
    ag = randint(18, 55)
    ci = randint(1, maxcity)
    return (na, fa, ag, ci)

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

class Position:
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


class Human:
    
    #дата начала работы
    # стажю. От него зависит вероятность продвижения по карьернойц лестнице
    # карьерная лестница: несколько ступеней, вероятность продвиджения на следуюшую ступень меньше, чем на предыдущую (*1.5)
    # талант: влияет на веротность повышения и на вероятность понижения
    HUM_COUNTER = 1
    def __init__(self, firms_pool):
        self.firms_pool = firms_pool
        self.fname = choice(first) #
        self.sname = choice(second) #
        self.lname = choice(last) #
        self.age = date(randint(1960, 1989), randint(1, 12), randint(1, 28)) # день рождения
        self.talent = randint(TALENT_MIN, TALENT_MAX)
        self.srart_work = self.age + timedelta(days=365*20)  # дата начала работы
        self.pos = Position()
        self.firm = choice(self.firms_pool)

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
        targ = choice(self.firms_pool)
        if self.firm != targ:
            attraction_mod = targ.attraction - self.firm.attraction 
            chanse = (40 + attraction_mod)/(40*365)
            if random() < chanse:
                self.firm = targ
                return True
        return False


    def __repr__(self):

        s = f'{self.lname} {self.fname} {self.sname}, {self.age}, талант:{self.talent} фирма: "{self.firm.name}" долж:{self.pos.posname}, стаж: {self.experience}'
        return s

class Route:
    def __init__(self, city1, city2, period):
        self.curent = city1
        self.target = city2
        self.period = period

    def update(self):
        if ANNO % self.period==0:
            self.move()

    def get_passangers(self):
        pass



class Firm:

    def __init__(self, id,  name):
        self.id = id
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

firms  = list()
for  n, name in enumerate(cit):
    fi = Firm(n+1, name)
    firms.append(fi)
    print(fi)





people = list()
for i in range(1000):
    people.append(Human(firms))


for t in range(365*7):
    ANNO += timedelta(days=1)
    for f in firms:
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

firms_count = {i:0 for i in firms}
for p in people:
    firms_count[p.firm] +=1

for f in firms_count:
    print(f'{f.name}  рпестиж: {f.attraction}   сотрудников: {firms_count[f]}')


for i in people[:16]:
    print(i)