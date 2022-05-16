from sqlalchemy.engine import Engine
from sqlalchemy import create_engine,  event
from sqlalchemy.orm import sessionmaker
import random
random.seed(667)



@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode = MEMORY")
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.close()


# engine = create_engine(f"sqlite:///workers.db", echo=True)
engine = create_engine(f"sqlite:///workers.db", echo=False)
from model import Base, bind_session


Session = sessionmaker()
Session.configure(bind=engine)
session = Session()


bind_session(session)

import settings
from settings import (SIM_YEARS, time_pass, YEAR_LENGTH,
                        INITIAL_PEOPLE_NUMBER,
                        INITIAL_FIRM_NUMBER,
                        POSITION_CAP,
                      )

from model.status import Status, statnames, StatusName, PeopleStatus
from model.worker_base import (
                               LastSimDate,
                               PosBase,
                               FirmName,
                               Position
                               )
from model.firm import Firm
from model.human import People

# удаляем все таблицы, чтобы введенные прежде данные не мешали
Base.metadata.drop_all(engine)


# создаем все таблицы
Base.metadata.create_all(engine)

def create_firm(name_id=None) -> Firm:
    firm_name_id = Firm.get_unused_firmname_id() if name_id==None else name_id
    Firm.mark_firmname_as_used(firm_name_id)
    fi = Firm(firm_name_id)
    session.add(fi)
    session.flush()
    return fi

def create_all_firms() -> list[ Firm]:
    # заполняем таблицу фирм
    # создаем названия фирм
    firmname_list = list()
    for name in settings.firm_names:
        fi = FirmName(name=name)
        firmname_list.append(fi)
    session.add_all(firmname_list)
    session.flush()
    del firmname_list

    # фейковая фирма для безработных
    # такая же, как и все остальные, просто ее особо отслеживать надо
    # первой делаем фирму с первым названием в списке имен фирм
    firms_list = list()

    for _ in range(INITIAL_FIRM_NUMBER):
        # выбираем директора - человека способного работать и не приписанного ни к какой фирме
        fi = create_firm()
        firms_list.append(fi)
    session.commit()
    return firms_list


def init_firm(firm, people):
    # выбираем директора - человека способного работать и не приписанного ни к какой фирме
    print(f'{firm=}')
    while True:
        director = random.choice(people)
        # трудоспособный и не является директором
        if director.age > 19 and director.pos.position != POSITION_CAP:
            break
    firm.initial_assign_director(director)

def firms_init(firms_list, people):
    for firm in firms_list:
        init_firm(firm, people)
    session.commit()


def create_postiton_names():
    for i in settings.position_names:
        session.add(PosBase(name = i))
    session.commit()

def create_staus_names():
    for i in Status:
        session.add(StatusName(id=i.value, name=statnames[i]))
    session.commit()

def create_people()->list[People]:
    '''
    Создаем людей не присваивая им никакие данные связанные с работой.
    '''
    people = list()
    for i in range(INITIAL_PEOPLE_NUMBER):
        people.append(People())
    session.add_all(people)
    session.flush()
    session.commit()
    return people


# инициируем людей
def people_init(people):
    for hum in people:
        hum.assign()
    session.commit()


def assign_people_to_firms(people):
    for hum in people:
        hum.unemployed_to_worker()
    session.commit()



create_postiton_names()
create_staus_names()

firm_list = create_all_firms()

People.obj_firms = firm_list
people =create_people()
people_init(people) # превоначальная инициация, все безработные

# следим за тем, чтобы у каждого человека была должность, даже должность "безработынй"
for i in people:
    assert i.pos is not None, f'у человека {i.id} не инициирована позиция'

Firm.obj_people = people
firms_init(firm_list, people) # здесь к фирме приписывается деректор из уже инициализированных взрослых безработных людей
assign_people_to_firms(people) # после того, как закрепили за фирмами директоров, устраиваем на работу всех остальных

lsd = LastSimDate()
session.add(lsd)
session.commit()

for t in range(int(YEAR_LENGTH * SIM_YEARS)):
    time_pass()
    lsd.date = settings.get_anno()

    for f in firm_list:
        f.update()
    for p in people:
        p.update()

    if len(firm_list) <15 and  random.random() < (1/90):
        print('создана новая фирма')
        f = create_firm()
        init_firm(f, people)
        firm_list.append(f)
    session.commit()

# ---------------------------------------------
# конец симуляции
# дальше будут статистические вычисления
