from datetime import date, timedelta

from sqlalchemy import create_engine, event, cast, Date
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, joinedload, aliased
from sqlalchemy.sql import func, distinct, exists, or_, and_
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
    personel_1 = dict()

    work_process = dates.copy()

    for r in rait_rec:
        personel_1[r.rate_date] = r.workers_count
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

    personel_2 = dict()
    for i in matches:
        alter_workers_count = None if matches[i] is None else dates[matches[i]]
        personel_2[i.rate_date] = alter_workers_count
        print(i.rate_date, matches[i], i.workers_count, alter_workers_count)
    return personel_1, personel_2


def hired_and_fired_on_specific_date(spec_firm, spec_date):
    # выбираем все записи, когда человек пришел в указанную фирму, а потом ушел из нее
    fired = aliased(PeopleFirm, name = 'fired')
    workers_in_out = (session.query(PeopleFirm.id)
               # прийти и уйти должен один человек
               # прийти и уйти нужно из одной и той эе фирмы
               .join(fired, and_(fired.people_id == PeopleFirm.people_id, fired.firm_from_id == PeopleFirm.firm_to_id))
               # дата ухода из фирмы должна быть позже даты прихода в фирму
               .filter(fired.move_to_firm_date > PeopleFirm.move_to_firm_date)
               # идетификатор фирмы и интересующая дата
               .filter(PeopleFirm.firm_to_id==spec_firm, PeopleFirm.move_to_firm_date <= spec_date)
               # уйти из фирмы человек должен вплоть до интересующей нас даты, иначе он в этой фирме работает на указанную дату
               .filter(fired.move_to_firm_date < spec_date)
               .order_by(PeopleFirm.people_id)
                      )
    b = workers_in_out.all()
    # берем все записи о приходе в конкретную фирму
    # и отнимаем все записи, где известно, что позже человек ушел из фирмы
    workers_in = (session.query(PeopleFirm.id)
                  .filter(PeopleFirm.firm_to_id == spec_firm)
                  .filter(PeopleFirm.move_to_firm_date <= spec_date)
                  .filter(PeopleFirm.id.notin_(workers_in_out))
                  .all())

    #print(workers_in)
    #print(f'фирма {spec_firm:3d} | {spec_date} Количество сотрудников: {len(workers_in):3d}')
    #print(b)
    return(len(workers_in))



# # проверить есть ли у кого-то на момент генерации статус UNEMPLOYED
# проверить, что у всех людей со статусом EMPLOYED фирма, к которой они приписаны не равна None

# сделать подсчет трудового опыта с учетом больничных и периодов безработности.


# young_people()
# able_people_on_start()
# unemp_after_sin_init()

# new_rating()
# странный_доступ_к_таблице()
MY_FIRM_ID = 2
# histiory_of_workers_in_firm(MY_FIRM_ID)
personel_1, personel_2 = worker_count_correlation_with_ratings(MY_FIRM_ID)


# hired_and_fired_on_specific_date(2, '2007-01-01')

personel_3 = dict()
rating_dates = session.query(distinct(FirmRating.rate_date)).filter(FirmRating.firm_id==MY_FIRM_ID).order_by(FirmRating.rate_date).all()
for i in rating_dates:
    personel_3[i[0]] = hired_and_fired_on_specific_date(MY_FIRM_ID, i[0])
print(personel_1)
print(personel_2)
print(personel_3)

for d in personel_3:
    p1 = personel_1[d] if personel_1.get(d) else None
    p2 = personel_2[d] if personel_2.get(d) else None
    p3 = personel_3[d]
    print(d, p1, p2, p3)
