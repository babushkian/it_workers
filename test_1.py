from datetime import date, timedelta

from sqlalchemy import create_engine, event, cast, Date
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, joinedload, aliased
from sqlalchemy.sql import func, distinct, exists, or_
from pathlib import Path
from model import Base, bind_session


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
bind_session(session)

from model.status import StatusName, PeopleStatus, Status
from model.human import People
from model.worker_base import (PosBase,
                               LastSimDate,
                               PeopleFirm, PeoplePosition,
                               Position,
                               FirmName,
                               FirmRating
                               )
from model.firm import Firm
from settings import START_DATE

# проверить, что у всех несовершенноетних на момент генерации стоит статус YOUNG
def young_people():
    # выборка имеющихся должностей
    print('=' * 20, '\nЛюди со статусом YOUNG на момент начала симуляции.')
    #psd = session.query(People).join(PeopleStatus).filter(PeopleStatus.status_id== Status.YONG).all()
    ppl = session.query(distinct(PeopleStatus.people_id)).filter(PeopleStatus.status_date=='2010-01-01', PeopleStatus.status_id<Status.UNEMPLOYED)
    psd = session.query(People).filter(People.id.in_(ppl)).all()
    for i in psd:
        print(i)

# проверить, что все совершеннолетние на момент генерации устроены на работу
def able_people_on_start():
    # выборка имеющихся должностей
    print('=' * 20, '\nЛюди со статусом EMPLOYED на момент начала симуляции.')

    ppl = session.query(distinct(PeopleStatus.people_id)).filter(PeopleStatus.status_date=='2010-01-01', PeopleStatus.status_id==Status.EMPLOYED)
    ppl_status = (session.query(PeopleStatus)
                  .filter(PeopleStatus.people_id.in_(ppl))
                  .order_by(PeopleStatus.people_id, PeopleStatus.status_date)
                  .all()
                  )
    for i in ppl_status:
        print(i.people_id, i.status_name.name, i.status_date)

#  поиск людей со статусом безоаботный после инициализации симуляции
def unemp_after_sin_init():
    ppl = session.query(PeopleStatus).filter(PeopleStatus.status_date >'2010-01-01', PeopleStatus.status_id==Status.UNEMPLOYED).order_by(PeopleStatus.people_id).all()
    if len(ppl)==0:
        print("Нет людей со статусом безработный после инициализации")
    else:
        for i in ppl:
            print(i.people_id, i.status_name.name, i.status_date)


# рейтинг всех фирм
def new_rating():
    # таблица рейтинга всех людей (добавляе поле рейтинг)
    rating =  session.query(People.id, People.current_firm_id, (People.talent*People.last_position_id).label('rating')).cte(name='rating')
    firm_rating =  (session.query(rating.c.current_firm_id.label('firm'), func.avg(rating.c.rating).label('firm_rating'), func.count(rating.c.id).label('people_count'))
        .filter(rating.c.current_firm_id != None)
        .group_by(rating.c.current_firm_id)
                    .all()
                    )
    for i in firm_rating:
        print(f'firm id: {i.firm:3d} rating: {i.firm_rating:6.2f} people in firm: {i.people_count:3d}')

    #print(dir(Base.metadata.tables['people']))

def странный_доступ_к_таблице():
    a = Base.metadata.tables['people']
    x = session.query(a).limit(10)
    for i in x:
        print(i.current_firm_id)


def histiory_of_workers_in_firm(firm_id=1):

    records = (session.query(PeopleFirm)
               .filter(or_(PeopleFirm.firm_from_id == firm_id, PeopleFirm.firm_to_id == firm_id))
               .order_by(PeopleFirm.move_to_firm_date)
               .all()
               )
    dates = dict()
    count = 0
    for i in records:
        if i.firm_from_id == firm_id:
            count -= 1
        if i.firm_to_id == firm_id:
            count += 1
        dates[i.move_to_firm_date] = count

    for d in dates:
        print(f'{d} - {dates[d]}')


def worker_count_correlation_with_ratings(firm_id):
    records = (session.query(PeopleFirm)
               .filter(or_(PeopleFirm.firm_from_id == firm_id, PeopleFirm.firm_to_id == firm_id))
               .order_by(PeopleFirm.move_to_firm_date)
               .all()
               )
    dates = dict()
    count = 0
    for i in records:
        if i.firm_from_id == firm_id:
            count -= 1
        if i.firm_to_id == firm_id:
            count += 1
        dates[i.move_to_firm_date] = count

    rait_rec = session.query(FirmRating).filter(FirmRating.firm_id == firm_id).order_by(FirmRating.rate_date).all()
    # for i in rait_rec:
    #     print(i.rate_date, i.rating, i.workers_count)
    matches = dict()

    work_process = dates.copy()

    for r in rait_rec:
        to_del = list()
        for d in sorted(work_process):
            if d > r.rate_date:
                break
            to_del.append(d)

        if len(to_del) > 0:
            matches[r] = to_del[-1]
        else:
            matches[r] = None
        for i in to_del:
            del work_process[i]

    for i in matches:
        alter_workers_count = None if matches[i] is None else dates[matches[i]]
        print(i.rate_date, matches[i], i.workers_count, alter_workers_count)



# # проверить есть ли у кого-то на момент генерации статус UNEMPLOYED
# проверить, что у всех людей со статусом EMPLOYED фирма, к которой они приписаны не равна None

# сделать подсчет трудового опыта с учетом больничных и периодов безработности.


# young_people()
# able_people_on_start()
# unemp_after_sin_init()

# new_rating()
# странный_доступ_к_таблице()
# histiory_of_workers_in_firm(16)
worker_count_correlation_with_ratings(7)