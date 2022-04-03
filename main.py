from sqlalchemy.engine import Engine
from sqlalchemy import create_engine,  event
from sqlalchemy.orm import sessionmaker



import settings
from settings import SIM_YEARS, time_pass
from worker_base import (Base,
                        Position,
                        PosBase,
                        Firm,
                        Human
                         )



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

# заполняем таблицу фирм
firms_list  = list()
for  name in settings.firm_names:
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
    people.append(Human(session, firm_dict,  settings.get_rand_firm_id()))
session.add_all(people)
session.commit()

for t in range(int(365 * SIM_YEARS)):
    time_pass()
    for f in firms_list:
        f.update()
    for p in people:
        p.update()
    session.commit()

# ---------------------------------------------
# конец симуляции
# дальше будут статистические вычисления
