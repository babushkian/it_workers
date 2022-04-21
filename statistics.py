from datetime import date

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func, distinct

from model.human import Human
from model.worker_base import (PosBase,
                               LastSimDate,
                               Firm,
                               HumanFirm, HumanPosition
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

x = (session.query(HumanFirm.human_id, HumanFirm.move_to_firm_date, HumanFirm.firm_id, Firm.name)
     .join(Firm)
     .filter(HumanFirm.human_id<11)
     .order_by(HumanFirm.human_id, HumanFirm.move_to_firm_date))
print(x)
x = x.all()
print(x)
for i in x:
    print(f'{i.human_id:3d}  {i.move_to_firm_date}  {i.firm_id:3d}  {i.name}')

print('-----------------------------')
print('Список людей родившихся с 1970 года')
print('-----------------------------')
lsd = session.query(LastSimDate.date).scalar()
x = session.query(Human).filter(Human.birth_date > date(1970, 1,1)).order_by(Human.birth_date)
print(x)
x = x.all()
for i in x:
    exp = (lsd - i.start_work).days if  i.start_work is not None else 0
    print(i, 'опыт:', exp)

print('-----------------------------')
x = session.query(HumanPosition).filter(HumanPosition.human_id==6).order_by(HumanPosition.move_to_position_date).all()
for i in x:
    print(i.pos_id, i.move_to_position_date)

print('-----------------------------')
print('Даты повышения человека')
print('Достаются через отношения')
h=session.query(Human).filter(Human.id == 2).one()
print(h)
for i in h.position:
    print(i.pos_id, i.move_to_position_date)


print('-----------------------------')
print('Вышедших на пенсию:', session.query(func.count(Human.retire_date)).scalar())
print('Умерших:', session.query(func.count(Human.death_date)).scalar())
print('')

print("Умерших после выхода на пенсию")
x = session.query(func.count(Human.id)).filter(Human.retire_date != Human.death_date).scalar()
print(x)
x = session.query(Human.id, Human.retire_date, Human.death_date).filter(Human.retire_date < Human.death_date).all()
for i in x:
    print(f'{i.id:4d}  {i.retire_date} -- {i.death_date}')

def age(birth, event):
    age = event.year - birth.year - ((event.month, event.day) < (birth.month, birth.day))
    return age


pens = session.query(Human.retire_date,Human.birth_date ).filter(Human.retire_date.is_not(None), Human.retire_date !=Human.death_date)
print(pens)
pens = pens.all()
print('Количество пенсионеров, вышедших на пенсию до смерти: ', len(pens))
rsum = 0
for i in pens:
    a = age(i.birth_date, i.retire_date)
    rsum += a



print('Средний возраст выхода на пенсию ', (rsum/len(pens) if len(pens) >0 else 0))

deaths = session.query(Human.death_date,Human.birth_date ).filter(Human.death_date.is_not(None))

deaths = deaths.all()

dsum = 0
for i in deaths:
    a = age(i.birth_date, i.death_date)
    dsum += a

print('Средний возраст смерти ', (dsum/len(deaths) if len(deaths) >0 else 0))
