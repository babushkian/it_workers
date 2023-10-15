import random
import sys

from sqlalchemy import  func

from settings import (SIM_YEARS, time_pass, YEAR_LENGTH,
                      INITIAL_PEOPLE_NUMBER,
                      POSITION_CAP,
                      get_anno,
                      firm_names,
                      position_names,
                      UNEMPLOYED_POSITION, MAX_FIRMS_QTY
                      )

from model import Base, session, engine
from model.status import Status, statnames, StatusName, PeopleStatus
from model.worker_base import (
                    LastSimDate,
                    PositionNames,
                    FirmName,
                    PeoplePosition, PeopleFirm,
                    Position, FirmRating
)
from model.firm import Firm
from model.human import People

# удаляем все таблицы, чтобы введенные прежде данные не мешали
Base.metadata.drop_all(engine)

# создаем все таблицы
Base.metadata.create_all(engine)

def create_postiton_names():
    '''
    наполняем таблицу PositionNames (positions) названиями должностей
    '''
    for i in position_names:
        x = PositionNames(name=i)
        session.add(x)
    session.commit()


def create_staus_names():
    '''
    наполняем таблицу StatusName (statuses)
    таблица содержит перечисление разных статусов человека: безработный, работает, пенсионер, умер
    фактически нужна для извлечения названий статусов человека
    '''
    for i in Status:
        session.add(StatusName(id=i.value, name=statnames[i]))
    session.commit()


def create_firm_names():
    '''
    Создается таблица с названиями фирм FirmName (firmnames)
    '''
    firmname_list = list()
    for name in firm_names:
        firmname_list.append(FirmName(name=name))
    session.add_all(firmname_list)
    session.commit()

def create_people(people_qty) -> list[People]:
    '''
    Создаем людей не присваивая им никакие данные связанные с работой.
    '''
    people = list()
    for i in range(people_qty):
        people.append(People())
    session.add_all(people)
    session.commit()
    return people


# инициируем людей
def people_init(people):
    for hum in people:
        hum.assign()
    session.commit()


def update_all_firms_ratings():
    rating = (
        session.query(People.id, People.current_firm_id, (People.talent * People.current_position_id).label('rating'))
        .filter(People.current_firm_id != None)
        .cte(name='rating')
        )
    firm_rating = (
        session.query(rating.c.current_firm_id.label('firm'), func.avg(rating.c.rating).label('firm_rating'),
                      func.count(rating.c.id).label('people_count'))
        .filter(rating.c.current_firm_id != None)
        .group_by(rating.c.current_firm_id)
        .all()
    )
    print(firm_rating)
    for i in firm_rating:
        if People.obj_firms[i.firm].close_date is None:
            session.add(
                FirmRating(firm_id=i.firm, rating=i.firm_rating, workers_count=i.people_count, rate_date=get_anno()))
            session.query(Firm).filter(Firm.id == i.firm).update({Firm.last_rating: i.firm_rating})
    session.commit()


if __name__ == "__main__":

    create_postiton_names()
    create_staus_names()
    create_firm_names()

    people = create_people(INITIAL_PEOPLE_NUMBER)
    Firm.obj_people = people
    people_init(people)  # превоначальная инициация, все безработные

    # следим за тем, чтобы у каждого человека была должность, даже должность "безработынй"
    for i in people:
        assert i.pos is not None, f'у человека {i.id} не инициирована позиция'


    lsd = LastSimDate()
    session.add(lsd)
    session.commit()

    for t in range(int(YEAR_LENGTH * SIM_YEARS)):
        print(get_anno(), "=============================")
        time_pass()
        lsd.date = get_anno()
        for p in people:
            p.update()

        if get_anno().day == 1:
            update_all_firms_ratings()

            for f in People.obj_firms.values():
                print('===========================')
                print(f)
                candidats = [i for i in Firm.obj_people if i.current_firm_id == f.id]
                for c in candidats:
                    print('    ', c.verbose_repr())

        session.commit()