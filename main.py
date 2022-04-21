from sqlalchemy.engine import Engine
from sqlalchemy import create_engine,  event
from sqlalchemy.orm import sessionmaker
import random
random.seed(666)



import settings
from settings import (SIM_YEARS, time_pass, YEAR_LENGTH,
                        INITIAL_PEOPLE_NUMBER,
                        INITIAL_FIRM_NUMBER,
                      )
from model.worker_base import (Base,
                               LastSimDate,
                               PosBase,
                               Firm, FirmName,
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

def create_firm() -> Firm:
    firm_id = Firm.get_unused_firm_id()
    fi = Firm(firm_id)
    session.add(fi)
    session.flush()
    fi.assign()
    session.flush()
    return fi

def create_all_firms() -> list[ Firm]:
    # заполняем таблицу фирм
    firms_list = list()
    fn_list = list()
    for name in settings.firm_names:
        fi = FirmName(name=name)
        fn_list.append(fi)
    session.add_all(fn_list)
    session.flush()

    Firm.bind_session(session)

    for n in range(INITIAL_FIRM_NUMBER):
        fi = create_firm()
        firms_list.append(fi)
    session.commit()
    return firms_list


def create_postiton_names():
    for i in settings.position_names:
        session.add(PosBase(name = i))
    session.commit()

# инициируем людей
def people_init():
    people = list()
    for i in range(INITIAL_PEOPLE_NUMBER):
        people.append(Human(session))
    session.add_all(people)
    session.flush()
    for hum in people:
        hum.assign()
    session.commit()
    return people




firm_list = create_all_firms()


Firm.get_used_firm_ids_pool()
create_postiton_names()
people = people_init()
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

    if random.random() < (1/50):
        print("создана новая фирма")
        firm_pool = Firm.get_used_firm_ids_pool()
        print(firm_pool)
        firm_list.append(create_firm())
    session.commit()

# ---------------------------------------------
# конец симуляции
# дальше будут статистические вычисления
