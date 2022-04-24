from datetime import date

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, joinedload
from sqlalchemy.sql import func, distinct, exists
from pathlib import Path
from model.human import People
from model.worker_base import (PosBase,
                               LastSimDate,
                               Firm,
                               PeopleFirm, PeoplePosition, FirmName
                               )


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode = MEMORY")
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.close()

basefile = 'mega_workers.db'
if Path(basefile).exists() == False:
    basefile ='workers.db'

engine = create_engine(f"sqlite:///{basefile}", echo=True)
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

def all_people_count():
    all_rec = session.query(People).count()
    print('-----------------------------')
    print(f'Всего записей о людях: {all_rec}')
    all_rec = session.query(func.count(People.id)).scalar()

    print(f'Количество записей, вычисленное другим способом, более быстрым: {all_rec}')


def mean_qualification():
    # средняя квалификация по всем людям
    print('=' * 20, '\nИдентификаторы должностей, имеющихся среди людей.')
    mean_pos = session.query(func.sum(People.last_position_id).label('p_sum'), func.count(People.last_position_id).label('p_count')).one()
    print(mean_pos)
    print(f"Среняя квалификация людей: {mean_pos.p_sum / mean_pos.p_count}")

def talent_mean_qualification():
    # средняя квалификация в зависимости от таланта
    p_tal = session.query(func.sum(People.last_position_id).label('tal_sum'), func.count(People.talent).label('tal_count'), People.talent)\
        .group_by(People.talent).order_by(People.talent).all()
    print('средняя квалификация в зависимости от таланта')
    for i in p_tal:
        print(i.talent, i.tal_sum/i.tal_count)

def real_positions():
    # выборка имеющихся должностей
    print('=' * 20, '\nИдентификаторы должностей, имеющихся среди людей.')
    psd = session.query(distinct(People.last_position_id)).order_by(People.last_position_id).all()
    print(psd)

def positions_distribution():
    # РАСПРЕДЕЛЕНИЕ ЛЮДЕЙ ПО ДОЛЖНОСТЯМ
    print('*' * 40, '\nРаспределение должностей среди людей')
    x = (session.query(func.count(People.id).label('cont'), PosBase.name)
         .join(PosBase)
         .group_by(People.last_position_id)
         .order_by(People.last_position_id).all()
         )
    for y in x:
        print(y.cont, y.name)


def real_firm_names():
    print('-----------------------------')
    print('Названия имеющихся фирм')
    print('-----------------------------')
    x = session.query(Firm).options(joinedload('firmname'),).all()
    for i in x:
        print(i.firmname.name)

def born_after_X_year():
    x= session.query(exists().where(People.birth_date >= date(1995, 1, 1))).scalar()
    print('-----------------------------')
    print('имеются ли люди младше 1994 года', x)

def people_from_firm_X():
    print('-----------------------------')
    print('Все люди, последним местом работы которых была фирма с id=1')
    print('-----------------------------')
    x= session.query(People).options(joinedload('firm_with_name')).filter(Firm.id == 1).all()
    for i in x:
        print(i)




# x = (session.query(PeopleFirm.people_id, PeopleFirm.move_to_firm_date, PeopleFirm.last_firm_id, Firm.name)
#      .join(Firm)
#      .filter(PeopleFirm.people_id<11)
#      .order_by(PeopleFirm.people_id, PeopleFirm.move_to_firm_date)
#      )

def firm_names_from_human_firms():
    print('-----------------------------')
    print('Фирмы, в которых работают люди с id<11')
    print('сделано через промежутачную таблицу human_firms, а не через таблицу people, что было бы логично')
    print('-----------------------------')
    # сначала выделяем список идентификаторов всех фирм в которых работают выбранные люди
    y = (session.query(PeopleFirm.firm_id)
         .filter(PeopleFirm.people_id < 11)
         .order_by(PeopleFirm.people_id)
         )
    # затем по нужным идентификаторам фирм получаем записи фирм
    x = session.query(Firm).options(joinedload('firmname')).filter(Firm.id.in_(y)).all()
    '''
    SELECT firms.id AS firms_id, firms.firmname_id AS firms_firmname_id, firms.last_rating AS firms_last_rating, firms.open_date AS firms_open_date, firms.close_date AS firms_close_date, firmnames_1.id AS firmnames_1_id, firmnames_1.name AS firmnames_1_name, firmnames_1.used AS firmnames_1_used 
    FROM firms JOIN firmnames AS firmnames_1 ON firmnames_1.id = firms.firmname_id 
    WHERE firms.id IN (
        SELECT people_firms.firm_id 
        FROM people_firms 
        WHERE people_firms.people_id < ? ORDER BY people_firms.people_id
        )
    '''
    for i in x:
        print(i)

'''
y = session.query(Firm).join(FirmName)
print(y)
x = (session.query(PeopleFirm.people_id, PeopleFirm.move_to_firm_date, PeopleFirm.last_firm_id, Firm.firmname_id, FirmName.name)
     .join(y)
     .filter(PeopleFirm.people_id<11)
     .order_by(PeopleFirm.people_id, PeopleFirm.move_to_firm_date))
print(x)
x = x.all()
print(x)
for i in x:
    print(f'{i.people_id:3d}  {i.move_to_firm_date}  {i.last_firm_id:3d}  {i.firmname_id}')
'''


all_people_count()
mean_qualification()
talent_mean_qualification()
real_positions()
positions_distribution()
x = session.query(2+1).scalar()
print(x)

'''
print('-----------------------------')
print('Список людей родившихся с 1970 года')
print('-----------------------------')
lsd = session.query(LastSimDate.date).scalar()
x = session.query(People).filter(People.birth_date > date(1970, 1,1)).order_by(People.birth_date)
print(x)
x = x.all()
for i in x:
    exp = (lsd - i.start_work).days if  i.start_work is not None else 0
    print(i, 'опыт:', exp)

print('-----------------------------')
x = session.query(PeoplePosition).filter(PeoplePosition.people_id==6).order_by(PeoplePosition.move_to_position_date).all()
for i in x:
    print(i.position_id, i.move_to_position_date)

print('-----------------------------')
print('Даты повышения человека')
print('Достаются через отношения')
h=session.query(People).filter(People.id == 2).one()
print(h)
for i in h.position:
    print(i.position_id, i.move_to_position_date)


print('-----------------------------')
print('Вышедших на пенсию:', session.query(func.count(People.retire_date)).scalar())
print('Умерших:', session.query(func.count(People.death_date)).scalar())
print('')

print("Умерших после выхода на пенсию")
x = session.query(func.count(People.id)).filter(People.retire_date != People.death_date).scalar()
print(x)
x = session.query(People.id, People.retire_date, People.death_date).filter(People.retire_date < People.death_date).all()
for i in x:
    print(f'{i.id:4d}  {i.retire_date} -- {i.death_date}')

def age(birth, event):
    age = event.year - birth.year - ((event.month, event.day) < (birth.month, birth.day))
    return age


pens = session.query(People.retire_date,People.birth_date ).filter(People.retire_date.is_not(None), People.retire_date !=People.death_date)
print(pens)
pens = pens.all()
print('Количество пенсионеров, вышедших на пенсию до смерти: ', len(pens))
rsum = 0
for i in pens:
    a = age(i.birth_date, i.retire_date)
    rsum += a



print('Средний возраст выхода на пенсию ', (rsum/len(pens) if len(pens) >0 else 0))

deaths = session.query(People.death_date,People.birth_date ).filter(People.death_date.is_not(None))

deaths = deaths.all()

dsum = 0
for i in deaths:
    a = age(i.birth_date, i.death_date)
    dsum += a

print('Средний возраст смерти ', (dsum/len(deaths) if len(deaths) >0 else 0))
'''