import datetime
from datetime import date

from sqlalchemy import create_engine, event, cast, Date, or_
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, joinedload, aliased
from sqlalchemy.sql import exists
from sqlalchemy import func, distinct
from sqlalchemy.sql import select, literal
from pathlib import Path
from model.human import People
from model.firm import Firm
from model.worker_base import (PositionNames,
                               LastSimDate,
                               PeopleFirm, PeoplePosition, FirmName, FirmRating
                               )

from settings import POSITION_CAP
my_user = 'root'
my_pass = 'ZalgoNickro666'

engine = create_engine(f'mysql+mysqlconnector://{my_user}:{my_pass}@localhost/workers', echo=False)



Session = sessionmaker()
Session.configure(bind=engine)
session = Session()


def all_people_count_2_style():
    # pc = select(People).count() # не работает, говорит у селекта нет метода каунт
    # pc = select(func.count()).select_from(People) # работает
    #pc = select(func.count(People))   не работает, говорит вместо выражения получила таблицу
    pc = select(func.count(People.first_name))
    res = session.execute(pc).scalar()

    print('-----------------------------')
    print(f'Всего записей о людях: {res}')


def mean_qualification_2_style():
    # средняя квалификация по всем людям
    print('=' * 20)
    pc = select(func.avg(People.current_position_id))
    res = session.execute(pc).scalar()
    print(f"Среняя квалификация людей: {res:5.3f}")


def talent_mean_qualification_2_style():
    # средняя квалификация в зависимости от таланта
    print('-----------------------------')


    pc = (select(func.avg(People.current_position_id).label('tal_avg'),
                 func.count(People.talent).label('tal_count'),
                 People.talent)
                .group_by(People.talent)
                .order_by(People.talent.desc())
                )   
    avg_tal = session.execute(pc)
    print(avg_tal.keys())
    print('Талант, средняя должность для таланта, кол-во людей с талантом')
    for i in avg_tal:
        print(f'{i.talent:3d} {i.tal_avg:6.3f} ({i.tal_count})')


def real_positions_2_style():
    # выборка имеющихся должностей
    print('=' * 20, '\nИдентификаторы должностей, имеющихся среди людей.')
    pc = select(distinct(People.current_position_id)).order_by(People.current_position_id)
    res = session.execute(pc).scalars()
    print(list(res))

def real_position_names_2_style():
    # выборка имеющихся должностей с имнеами
    print('=' * 20, '\nназвания должностей, имеющихся в данный момент среди людей.')
    # sq = select(distinct(People.current_position_id)).subquery()
    sq = select(distinct(People.current_position_id).label('position_id')).subquery()
    s = session.execute(select(sq)).scalars()
    print(list(s))




def positions_distribution_2_style():
    # РАСПРЕДЕЛЕНИЕ ЛЮДЕЙ ПО ДОЛЖНОСТЯМ
    print('*' * 40, '\nРаспределение должностей среди людей')
    pc = (select(func.count(People.id).label('cont'), PositionNames.name, People.current_position_id)
         .join(PositionNames)
         .group_by(People.current_position_id)
         .order_by(People.current_position_id)
         )
    res = session.execute(pc)
    for y in res:
        print( y.current_position_id, y.name, " -- ", y.cont)



def real_firm_names_2_style():
    print('-----------------------------')
    print('Названия имеющихся фирм')
    print('-----------------------------')
    q = select(Firm).options(joinedload(Firm.firmname),)
    res = session.execute(q).scalars()
    for i in res:
        print(i.firmname.name)



def real_firm_names_2_style_join():
    print('-----------------------------')
    print('Названия имеющихся фирм')
    print('-----------------------------')
    q = select(Firm, FirmName).join(Firm.firmname).order_by(Firm.open_date, FirmName.name)
    res = session.execute(q)
    print(res)
    print(res.keys())
    for i in res:
        print(i.Firm.id, i.FirmName.name, i.FirmName.used, i.Firm.open_date)

def born_after_X_year_2_style():
    q = select(exists().where(People.birth_date >= date(1995, 1, 1)))
    x = session.execute(q).scalar()
    print('имеются ли люди младше 1994 года', x)
    print('сколько людей родившихся после 1994 года')
    q = select(func.count(People.id)).where(People.birth_date >= date(1995, 1, 1))
    x = session.execute(q).scalar()
    print('Их количество:',x)
    # другой подход с использованием метода select_from
    q = select(func.count()).select_from(People).where(People.birth_date >= date(1995, 1, 1))
    x = session.execute(q).scalar()
    print('Их количество:',x)

    

def people_from_firm_X():
    print('-----------------------------')
    print('Все люди, последним местом работы которых была фирма с id=1')
    print('-----------------------------')
    x = (session.query(People).join(Firm)
         .options(
            joinedload(People.recent_firm),
            joinedload(People.position_name),
            )
         .filter(Firm.id == 1).all()
         )
    for i in x:
        print(i)
    print('Количество записей: о людях, работающих в этой фирме: ', len(x))
    print("Другой Подход:")
    res = session.scalars(select(Firm).join(FirmName))
    for i in res:
        print(i.firmname.name)
        for p in i.current_emploees:
            print(p.verbose_repr())


def firm_chronology(my_firm_id):
    # уникальные даты поступления сотрудников в конкретную фирму
    q = (select(distinct(PeopleFirm.move_to_firm_date.label('md')))
         .where(PeopleFirm.firm_to_id == my_firm_id).order_by('move_to_firm_date'))

    pers_move_dates = session.execute(q).scalars().all()
    print(pers_move_dates)

    a = pers_move_dates[-2]
    for a in pers_move_dates:
        # записи о сотрудниках, поступавших когда-либо на службу в фирму
        pfrec = (select(PeopleFirm)
                    .where(or_(PeopleFirm.firm_to_id == my_firm_id,
                        PeopleFirm.firm_from_id == my_firm_id))
                    .where(PeopleFirm.move_to_firm_date <= a)
                    .cte('people_in_firm_ever')
                )
        # идентификаторы сотрудников, когда-либо бывших в фирме
        ids = select(distinct(pfrec.c.people_id).label('people_id')).cte('people_in_firm_ids')
        # последнаяя дата устройства человека в данную фирму
        hire_date = (select(func.max(pfrec.c.move_to_firm_date))
                     .where(pfrec.c.people_id == ids.c.people_id)
                     .where(pfrec.c.firm_to_id.is_not(None))
                     .scalar_subquery().correlate(ids)
        )
        # последнаяя дата увольнения человека из данной фирмы
        fire_date = (select(func.max(pfrec.c.move_to_firm_date))
                     .where(pfrec.c.people_id == ids.c.people_id)
                     .where(pfrec.c.firm_from_id.is_not(None))
                     .scalar_subquery().correlate(ids)
        )
        # записи о людях, когда-либо работавших в данной фирме следующей информацией:
        # с датой последнего увольнения из данной фирмы (на искомую дату)
        # если человек никогда не увольнялся из данной фирмы, то там будет None,
        # поэтому присваиваем этой ячейке 1900 год, чтобы можно было сравнивать дату увольнения с датой зачисления
        # ну и последняя дата зачисления в данную фирму
        people_with_dates = (select(ids,
                                    func.coalesce(fire_date, datetime.date(1900, 1,1)).label('fire_date'),
                                    hire_date.label('hire_date')).select_from(ids).cte()
                             )
        # список работающих в фирме людей на интересующий нас момент
        # дата последнего зачисления в искомую фирму должна быть больше даты последнего увольения
        working_people = (select(people_with_dates.c.people_id)
             .where(people_with_dates.c.hire_date > people_with_dates.c.fire_date)).cte()

        # даты назначения в директора тех сотрудников, которые в данный момент работают в фирме
        dir_dates = (select(PeoplePosition.people_id, PeoplePosition.move_to_position_date)
                     .where(PeoplePosition.people_id.in_(select(working_people)))
                     .where(PeoplePosition.move_to_position_date <= a)
                     .where(PeoplePosition.position_id == POSITION_CAP)
                     ).cte()

        unique_directors = (select(dir_dates.c.people_id, func.max(dir_dates.c.move_to_position_date).label('mtp'))
                            .group_by(dir_dates.c.people_id)).cte()

        director_on_date = (select(literal(my_firm_id).label('firm_id'), literal(a.strftime('%Y.%m.%d')).label('check_date'),
                        unique_directors.c.mtp,
                        people_with_dates.c.people_id,
                        people_with_dates.c.hire_date)
                    .join_from(unique_directors, people_with_dates,
                               unique_directors.c.people_id==people_with_dates.c.people_id)
                    .where(unique_directors.c.mtp >= people_with_dates.c.hire_date)
             ).cte()

        q = select(distinct(director_on_date.c.people_id)).select_from(director_on_date)
        res = session.execute(q).scalars().all()
        print(res, len(res))
        print('====================================================')
        for i in res:
            print( i)

def records_per_man():
    csq = (select(func.count(PeopleFirm.people_id))
           .where(PeopleFirm.people_id == People.id).scalar_subquery().correlate(People)
           )
    print(csq, '\n\n')
    # q = select(People.id, csq.c.co).select_from(People)
    q = select(People.id, csq.label('co'))
    print(q)
    res = session.execute(q).all()
    print(res)


def firm_names_from_human_firms():
    print('-----------------------------')
    print('Фирмы, в которых работают люди с id<11')
    print('-----------------------------')

    ssq = select(PeopleFirm.id).where(PeopleFirm.people_id<11).subquery()
    sq = select(PeopleFirm.id).where()
    numbered = aliased(PeopleFirm, sq)
    q = select(Firm).join(numbered)
    print(q)
    res = session.execute(q)
    for i in res:
        print(i)
    '''
    # затем по нужным идентификаторам фирм получаем записи фирм
    x = session.query(Firm).options(joinedload('firmname')).filter(Firm.id.in_(y)).all()
    for i in x:
        print(i)
    '''

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
        joinedload("recent_firm.firmname"),  # видимо это свойство еще не объявлено, поэтому не резолвится через атрибут класса
        joinedload(People.position_name),
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
    x = session.query(PeoplePosition).filter(PeoplePosition.people_id==50).order_by(PeoplePosition.move_to_position_date).all()
    for i in x:
        print(i.position_id, i.move_to_position_date)


    print('-----------------------------')
    print('Даты повышения человека --- сложное соединение с явеым указанием усовмй соединения')
    h = (session.query(People, PeoplePosition.position_id, PeoplePosition.move_to_position_date, PositionNames.name)
         .join(PeoplePosition, PeoplePosition.people_id == People.id)
         .join(PositionNames, PositionNames.id == PeoplePosition.position_id)
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
    x = session.query(People.id, People.retire_date, People.death_date).filter(People.retire_date < People.death_date).all()
    for i in x:
        print(f'{i.id:4d}  {i.retire_date} -- {i.death_date}')


def average_retire_age():
    pens = session.query(People.retire_date,People.birth_date ).filter(People.retire_date.is_not(None), People.retire_date !=People.death_date)
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
    deaths = (session.query(People.death_date - People.birth_date)
              .filter(People.death_date.is_not(None))
              )

    mean_deaths = (session.query(func.avg(People.death_date - People.birth_date))
              .filter(People.death_date.is_not(None)).scalar()
              )

    print('Средний возраст смерти:', mean_deaths)

    for i in deaths:
        print((i[0]))


# all_people_count_2_style()
# mean_qualification_2_style()
# talent_mean_qualification_2_style()
# real_positions_2_style()
#
# real_position_names_2_style()
#
#
# positions_distribution_2_style()
# real_firm_names_2_style()
# real_firm_names_2_style_join()
# born_after_X_year_2_style()
# people_from_firm_X()
# records_per_man()
firm_chronology(8)
# firm_names_from_human_firms()
# ever_worked_in_firm_X()
# people_yonger_than_1970()
# promotion_dates()
# retired_and_dead()
#average_retire_age()
# average_death_age_dumb()
# average_death_age()

"""
pc = select(Firm, FirmName).join(Firm.firmname).join(Firm.ratings).order_by(Firm.id)

res = session.execute(pc).scalars()

for y in res:
    print( y, y.id, y.firmname.name)
    for r in y.ratings:
        print(r.rate_date, r.rating)


res = session.execute( select(LastSimDate)).scalars()
for y in res:
    print(y.id, y.date)
"""