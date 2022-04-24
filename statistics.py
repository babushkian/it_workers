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
    print('=' * 20)
    # mean_pos = session.query(func.sum(People.last_position_id).label('p_sum'), func.count(People.last_position_id).label('p_count')).one()
    avg_pos = session.query(func.avg(People.last_position_id)).scalar()

    #print(f"Среняя квалификация людей: {mean_pos.p_sum / mean_pos.p_count}")
    print(f"Среняя квалификация людей: {avg_pos:5.3f}")

def talent_mean_qualification():
    # средняя квалификация в зависимости от таланта
    p_tal = session.query(func.sum(People.last_position_id).label('tal_sum'), func.count(People.talent).label('tal_count'), People.talent)\
        .group_by(People.talent).order_by(People.talent).all()
    print('средняя квалификация в зависимости от таланта')
    for i in p_tal:
        print(i.talent, i.tal_sum/i.tal_count)
    print('-----------------------------')
    print('То же самое, но короче')
    avg_tal = (session.query(func.avg(People.last_position_id).label('tal_avg'), func.count(People.talent).label('tal_count'), People.talent)
                .group_by(People.talent).order_by(People.talent).all()
               )
    for i in avg_tal:
        print(f'{i.talent:3d} {i.tal_avg:6.3f} ({i.tal_count})')



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
    x = session.query(func.count(People.id)).filter(People.birth_date >= date(1995, 1, 1)).scalar()
    print('Их количество:',x)

def people_from_firm_X():
    print('-----------------------------')
    print('Все люди, последним местом работы которых была фирма с id=1')
    print('-----------------------------')
    x = (session.query(People).join(Firm)
         .options(
            joinedload(People.recent_firm),
            joinedload("recent_firm.firmname"), # видимо это свойство еще не объявлено, поэтому не резолвится через атрибут класса
            joinedload(People.position_name),
            )
         .filter(Firm.id == 1).all()
         )
    for i in x:
        print(i)
    print('Количество записей: о людях, работающих в этой фирме: ', len(x))



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

    for i in x:
        print(i)


def ever_worked_in_firm_X():
    x = (session.query(Firm)
         .options(joinedload('people'),
                  joinedload('firmname'),
                  ).filter(Firm.id >7))
    print(x)
    x = x.all()

    for i in x:
        print('======================')
        print(i)
        print('======================')
        for j in i.people:
            print(j.human_conn)



def people_yonger_than_1970():
    print('-----------------------------')
    print('Список людей родившихся с 1970 года')
    print('-----------------------------')
    lsd = session.query(LastSimDate.date).scalar()

    x = (session.query(People).options(
        joinedload(People.recent_firm),
        joinedload("recent_firm.firmname"),  # видимо это свойство еще не объявлено, поэтому не резолвится через атрибут класса
        joinedload(People.position_name),
    )
         .filter(People.birth_date > date(1970, 1,1))
         .order_by(People.birth_date).all()
         )

    for i in x:
        exp = (lsd - i.start_work).days if  i.start_work is not None else 0
        print(i, 'опыт:', exp)

#
# all_people_count()
# mean_qualification()
# talent_mean_qualification()
# real_positions()
# positions_distribution()
# real_firm_names()
born_after_X_year()
# people_from_firm_X()
# firm_names_from_human_firms()
# ever_worked_in_firm_X()
people_yonger_than_1970()
'''
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