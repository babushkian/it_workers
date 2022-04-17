from sqlalchemy.engine import Engine
from sqlalchemy import create_engine,  event
from sqlalchemy.orm import sessionmaker
import random
random.seed(666)



import settings
from settings import (SIM_YEARS, time_pass, YEAR_LENGTH,
                        INITIAL_PEOPLE_NUMBER
                      )
from model.worker_base import (Base,
                               Position,
                               PosBase,
                               Firm,
                               )
from model.human import Human


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

def create_all_firms() -> dict[int, Firm]:
    # заполняем таблицу фирм
    firms_list  = list()
    for name in settings.firm_names:
        fi = Firm(session, name)
        firms_list.append(fi)
        session.add(fi)
        fi.assign()

    session.commit()

    firm_dict: dict[int, Firm] = {}
    for i in firms_list:
        firm_dict[i.id] = i
    return firm_dict


def create_postiton_names():
    for i in Position.POSITIONS:
        session.add(PosBase(name = i))
    session.commit()



firm_dict = create_all_firms()
create_postiton_names()

people = list()

# инициируем людей
for i in range(INITIAL_PEOPLE_NUMBER):
    people.append(Human(session))
session.add_all(people)
session.commit()

# дополнительно инициируем те опции, котоыре нельзя инициироватль сразу, потому что записи
# о человеке в базу не внесены
for i in people:
    i.assign()
session.add_all(people)
session.commit()


for t in range(int(YEAR_LENGTH * SIM_YEARS)):
    time_pass()
    for f in firm_dict.values():
        f.update()
    for p in people:
        p.update()
    session.commit()

# ---------------------------------------------
# конец симуляции
# дальше будут статистические вычисления
