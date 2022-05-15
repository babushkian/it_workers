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
                  .order_by(PeopleStatus.people_id)
                  .all()
                  )
    for i in ppl_status:
        print(i.people_id, i.status_name.name, i.status_date)

# проверить есть ли у кого-то на момент генерации статус UNEMPLOYED
# проверить, что у всех людей со статусом EMPLOYED фирма, к которой они приписаны не равна None



#young_people()
able_people_on_start()