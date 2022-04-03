from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func, asc, desc, distinct

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

engine = create_engine(f"sqlite:///workers.db", echo=False)
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

all_rec = session.query(Human).count()
print(f'Всего записей: {all_rec}')


# средняя квалификация по всем людям
print('=' * 20, '\nИдентификаторы должностей, имеющихся среди людей.')
mean_pos = session.query(func.sum(Human.pos_id).label('p_sum'),func.count(Human.pos_id).label('p_count')).one()
print(mean_pos)

print(f"Среняя квалификация людей: {mean_pos.p_sum / mean_pos.p_count}")


# средняя квалификация в зависимости от таланта
p_tal = session.query(func.sum(Human.pos_id).label('tal_sum'), func.count(Human.talent).label('tal_count'), Human.talent)\
    .group_by(Human.talent).order_by(Human.talent).all()
print('средняя квалификация в зависимости от таланта')
for i in p_tal:
    print(i.talent, i.tal_sum/i.tal_count)

# выборка имеющихся должностей
print('=' * 20, '\nИдентификаторы должностей, имеющихся среди людей.')
psd = session.query(distinct(Human.pos_id)).order_by(Human.pos_id).all()
print(psd)

# РАСПРЕДЕЛЕНИЕ ЛЮДЕЙ ПО ДОЛЖНОСТЯМ
# первый способ
print('*' * 40, '\nРаспределение должностей среди людей')
x = session.query(func.count(Human.id).label('cont'), PosBase.name).join(PosBase).group_by(Human.pos_id).order_by(
    Human.pos_id).all()
for y in x:
    print(y.cont, y.name)
