from datetime import date, timedelta

from sqlalchemy import create_engine, event, cast, Date
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, joinedload
from sqlalchemy.sql import func, distinct, exists, or_
from pathlib import Path
from model.human import People
from model.worker_base import (PosBase,
                               LastSimDate,
                               Firm,
                               PeopleFirm, PeoplePosition,
                               Position,
                               FirmName,
                               FirmRating
                               )
from settings import UNEMPLOYED

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

engine = create_engine(f"sqlite:///{basefile}", echo=False)
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

Firm.bind_session(session)


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


def firm_names_from_human_firms():
    print('-----------------------------')
    print('Фирмы, в которых работают люди с id<11')
    print('сделано через промежутачную таблицу human_firms, а не через таблицу people, что было бы логично')
    print('-----------------------------')
    # сначала выделяем список идентификаторов всех фирм в которых работают выбранные люди
    y = (session.query(distinct(PeopleFirm.firm_id))
         .filter(PeopleFirm.people_id < 11)
         .order_by(PeopleFirm.people_id)
         )
    # затем по нужным идентификаторам фирм получаем записи фирм
    x = session.query(Firm).options(joinedload('firmname')).filter(Firm.id.in_(y)).all()

    for i in x:
        print(i)


def ever_worked_in_firm_X():
    x = (session.query(Firm)
         .options(joinedload('people'),# это промежуточная таблица, а не people
                  joinedload('firmname'),
                  ).filter(Firm.id >7))
    print(x)
    x = x.all()

    for i in x:
        print('======================')
        print(i)
        print('======================')
        for j in i.people: # people_firm вот така последовательность обращений через промеж таблицу
            print(j.human_conn) # people



def people_yonger_than_1970():
    print('-----------------------------')
    print('Список людей родившихся с 1970 года')
    print('-----------------------------')
    lsd = session.query(LastSimDate.date).scalar()

    x = (session.query(People).options(
        joinedload(People.recent_firm),
        joinedload("recent_firm.firmname"),  # видимо это свойство еще не объявлено,
        joinedload(People.position_name),    # поэтому не резолвится через атрибут класса
    )
         .filter(People.birth_date > date(1970, 1,1))
         .order_by(People.birth_date).all()
         )

    for i in x:
        exp = (lsd - i.start_work).days if  i.start_work is not None else 0
        print(i, 'опыт:', exp)


def promotion_dates():
    print('-----------------------------')
    print('Даты повышения человека - простой способ, без промежуточной таблицы')
    x = (session.query(PeoplePosition)
         .filter(PeoplePosition.people_id==50)
         .order_by(PeoplePosition.move_to_position_date)
         .all()
         )
    for i in x:
        print(i.position_id, i.move_to_position_date)


    print('-----------------------------')
    print('Даты повышения человека --- сложное соединение с явеым указанием усовмй соединения')
    h = (session.query(People, PeoplePosition.position_id, PeoplePosition.move_to_position_date, PosBase.name)
         .join(PeoplePosition, PeoplePosition.people_id == People.id)
         .join(PosBase, PosBase.id == PeoplePosition.position_id)
         .filter(People.id == 50).all()
         )
    for i in h:
        print(i.People.first_name, i.People.last_name, i.position_id, i.name, i.move_to_position_date)

def age(birth, event):
    age = event.year - birth.year - ((event.month, event.day) < (birth.month, birth.day))
    return age

def retired_and_dead():
    print('-----------------------------')
    print('Вышедших на пенсию:', session.query(func.count(People.retire_date)).scalar())
    print('Умерших:', session.query(func.count(People.death_date)).scalar())
    print('')


    print("Умерших после выхода на пенсию")
    x = session.query(func.count(People.id)).filter(People.retire_date != People.death_date).scalar()
    print(x)
    x = (session.query(People.id, People.retire_date, People.death_date)
            .filter(People.retire_date < People.death_date)
            .all()
         )
    for i in x:
        print(f'{i.id:4d}  {i.retire_date} -- {i.death_date}')


def average_retire_age():
    pens = (session.query(People.retire_date,People.birth_date )
            .filter(People.retire_date.is_not(None),
                    People.retire_date !=People.death_date))
    print(pens)
    pens = pens.all()
    print('Количество пенсионеров, вышедших на пенсию до смерти: ', len(pens))

    rsum = 0
    for i in pens:
        a = age(i.birth_date, i.retire_date)
        rsum += a
    print('Средний возраст выхода на пенсию ', (rsum/len(pens) if len(pens) >0 else 0))


def average_death_age_dumb():
    deaths = session.query(People.death_date, People.birth_date).filter(People.death_date.is_not(None))

    deaths = deaths.all()

    dsum = 0
    for i in deaths:
        a = age(i.birth_date, i.death_date)
        dsum += a

    print('Средний возраст смерти ', (dsum / len(deaths) if len(deaths) > 0 else 0))

def average_death_age():
    # в sqlite округляет даты до годов
    deaths = (session.query((People.death_date - People.birth_date))
              .filter(People.death_date.is_not(None))
              )

    mean_deaths = (session.query(func.avg(People.death_date - People.birth_date))
              .filter(People.death_date.is_not(None)).scalar()
              )

    print('Средний возраст смерти:', mean_deaths)

    for i in deaths:
        print(i)

def annual_avg_age():
    print('------------------')
    print('Средний возраст:')

    x = session.query(distinct(FirmRating.rdate)).filter(FirmRating.rdate.like('%01-01%')).all()
    for i in x:
        check_date=i[0]
        y = (session.query(People.birth_date)
             .filter(or_(
                People.death_date == None,
                People.death_date > check_date)
            ).all())
        cumsum=0
        for d in y:
            cumsum += age(d[0], check_date )
        avg_age = cumsum/len(y)
        print(f'{check_date} {avg_age:5.1f}')


def directors_migrations():
    print('------------------')
    print('Все переходы директоров из одной фирмы в другую:')
    print('------------------')
    print('Идентификаторы директоров:')
    y = session.query(PeoplePosition.people_id).filter(PeoplePosition.position_id== Position.CAP).all()
    print(y)
    directors = session.query(PeoplePosition).filter(PeoplePosition.position_id== Position.CAP).subquery()
    # здесь у на сиспользуется связанный подзапрос
    x = (session.query(PeopleFirm.firm_id, PeopleFirm.people_id, PeopleFirm.move_to_firm_date)
         .join(directors, PeopleFirm.people_id == directors.c.people_id)
         .order_by(PeopleFirm.move_to_firm_date, PeopleFirm.firm_id).all()
         )
    for i in x:
        print(i)


def directors_migrations2():
    print('------------------')
    print('Все переходы директоров из одной фирмы в другую:')
    print('То же самое, что в проедыдущей функции но без использования подзапроса')
    print('Не совсем то же, отсеены те записи о людях, когда они еще не были директорами')
    print('------------------')
    print('Идентификаторы директоров:')

    y = session.query(PeoplePosition.people_id).filter(PeoplePosition.position_id== Position.CAP).all()
    print(y)
    # без подзапроса
    x = (session.query(PeopleFirm, PeoplePosition)
         .join(PeoplePosition, PeopleFirm.people_id == PeoplePosition.people_id)
         # челвоек является директором и не нужно учитывать записи о переходе в фирмы до той даты, как он стал директором
         .filter(PeoplePosition.position_id==Position.CAP, PeoplePosition.move_to_position_date <= PeopleFirm.move_to_firm_date)
         .order_by(PeopleFirm.id, PeopleFirm.firm_id).all()

         )

    for i in x:
        print(f'{i.PeopleFirm.id=:3d} {i.PeoplePosition.id=:3d} | {i.PeopleFirm.firm_id:3d} | {i.PeopleFirm.people_id=:3d}  {i.PeoplePosition.position_id=:3d}  {i.PeoplePosition.move_to_position_date} {i.PeopleFirm.move_to_firm_date}')


def show_directors_with_dates():
    print('\nСписок директоров и даты их назначения')
    x = (session.query(PeoplePosition)
         .filter(PeoplePosition.position_id==Position.CAP)
         .order_by(PeoplePosition.move_to_position_date).all()
         )
    for i in x:
        print(f'id:{i.people_id}, date: {i.move_to_position_date}')


def history_of_directors():
    print('\nНовооткрытые фирмы')
    x = (session.query(Firm)
         .filter(Firm.open_date > '2010-01-01')
         .order_by(Firm.open_date).all()
         )
    for i in x:
        print(f'id:{i.id}, date: {i.open_date}')

    print('\nНАзначение директоров')
    x = (session.query(PeoplePosition)
         .filter(PeoplePosition.position_id == Position.CAP, PeoplePosition.move_to_position_date > '2010-01-01')
         .order_by(PeoplePosition.move_to_position_date).all()
         )
    for i in x:
        print(f'id:{i.people_id}, date: {i.move_to_position_date}')

    print('\n Карьерная история директоров')
    y = (session.query(PeoplePosition.people_id)
         .filter(PeoplePosition.position_id == Position.CAP, PeoplePosition.move_to_position_date > '2010-01-01')
         )
    x = (session.query(PeoplePosition)
         .filter(PeoplePosition.people_id.in_(y))
         .order_by(PeoplePosition.people_id, PeoplePosition.move_to_position_date)
         )
    print(x)
    x = x.all()
    for i in x:
        print(f'id:{i.people_id:3d}, position: {i.position_id:3d},  date: {i.move_to_position_date}')

    print('\nВ каких фирмах работали директора')
    x = (session.query(PeopleFirm)
         .filter(PeopleFirm.people_id.in_(y))
         .order_by(PeopleFirm.people_id, PeopleFirm.move_to_firm_date)
         .all()
         )
    for i in x:
        print(f'id:{i.people_id:3d}, firm: {i.firm_id:3d},  date: {i.move_to_firm_date}')

def retired_directors():
    print('\nОтошедшие от дел директора')

    x = (session.query(People)
             .join(PeoplePosition)
             .filter(PeoplePosition.position_id==Position.CAP)
             .filter(People.retire_date != None)
         )
    print(x)
    x = x.all()
    for i in x:
        print(f'id:{i.id}')

def firms_without_directors():
    dir_ids = (session.query(PeoplePosition.people_id, PeoplePosition.move_to_position_date)
             .filter(PeoplePosition.position_id==Position.CAP).subquery()
               )

    print(dir_ids)

    ret_dirs = (session.query(PeopleFirm.people_id)
                .join(dir_ids, PeopleFirm.people_id == dir_ids.c.people_id)
                # дата назначения на должность директора в фирме должна быть ранеьше даты ухода из фирмы
                .filter( dir_ids.c.move_to_position_date < PeopleFirm.move_to_firm_date)
                )

    print(type(ret_dirs))
    firms_with_ret_dirs = (session.query(PeopleFirm)
                           .filter(PeopleFirm.people_id.in_(ret_dirs))
                           .filter(PeopleFirm.firm_id != UNEMPLOYED)
                           )
    print(type(firms_with_ret_dirs))


    for i in firms_with_ret_dirs:
        print(i.people_id, i.firm_id, i.move_to_firm_date)

# all_people_count()
# mean_qualification()
# talent_mean_qualification()
# real_positions()
# positions_distribution()
# real_firm_names()
# born_after_X_year()
# people_from_firm_X()
#firm_names_from_human_firms()
# ever_worked_in_firm_X()
# people_yonger_than_1970()
# promotion_dates()
# retired_and_dead()
# average_retire_age()
# average_death_age_dumb()
# average_death_age()
# annual_avg_age()
# directors_migrations()
# directors_migrations2()
# show_directors_with_dates()
history_of_directors()
retired_directors()
# firms_without_directors() # не заработала как надо
# самый старый живой человек. Надо считать тсходя из того, умер о или нет. Если не умер, то дату
# рождения отнимаем от плсоедней даты симуляции
# ежегодный отчет по количеству сотрудников в фирмах
# как часто люди переходят из одной фирмы в другую?
