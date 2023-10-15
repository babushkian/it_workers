from datetime import timedelta
from sqlalchemy import select, and_, exists, func
from sqlalchemy.orm import aliased, Session
import add_outer_dir                         # нужна для импорта модуля из внешнего каталога
import main_mysql as mm
from main_mysql import session
from mytest.test_init_people import (check_people, check_nobody_works, check_status_counts,
                                     check_no_workers_in_firms, check_no_positions, check_one_status_record_per_person)


def create_firms():
    mm.create_postiton_names()
    mm.create_staus_names()
    mm.create_firm_names()
    firm_dict = mm.create_all_firms(mm.INITIAL_FIRM_NUMBER)
    return firm_dict

def create_people(firm_dict):
    mm.People.obj_firms = firm_dict
    people = mm.create_people(mm.INITIAL_PEOPLE_NUMBER)
    mm.people_init(people)  # превоначальная инициация, все безработные
    return people

    # ==========================================
    # тестируем код, начинающийся отсюда
def init_firms(people):
    mm.Firm.obj_people = people
    # здесь к фирме приписывается директор из уже инициализированных взрослых безработных людей
    mm.firms_init(firm_dict, people)
    # после того, как закрепили за фирмами директоров, устраиваем на работу всех остальных
    mm.assign_people_to_firms(people)
    session.add(mm.LastSimDate())
    session.commit()

def addon():
    # создание ошибки
    # добавляем второго дтректора в фирму
    p = mm.Firm.obj_people[55] # конкретный человек
    f = p.obj_firms[1] # конкретная фирма
    f.initial_assign_director(p)

def check_staff(session:Session):
    ''''Обращение к таблицам через отношения'''
    print("=" * 30)
    with session:
        q = select(mm.Firm).join(mm.People).group_by(mm.Firm.id)
        # q = select(mm.Firm)
        res = session.execute(q).scalars()
        for i in res:
            print(i.firmname.name, i.current_emploees)
            # print(i.people)


def number_of_emploees_if_firms(session:Session):
    """
    Количество сотрудников в каждой фирме
    Нужно добавить неработающих чтобы при сложении число совпадало с общим числом людей
    """
    print("=" * 30)
    with session:
        q = select(mm.Firm)
        res = session.execute(q).scalars()
        people_number = session.scalar(select(func.count(mm.People.id)))
        print("Количество людей:", people_number)
        # неверно, он не отбирает последние состояния
        young_people_number = session.scalar(select(func.count(mm.People.id)).where(mm.People.current_status_id == 2))
        print("Количество молодых:", young_people_number)
        working = session.scalar(select(func.count(mm.People.id)).where(mm.People.current_status_id == 4))
        emp = 0
        for i in res:
            emp_in_firm = len(i.current_emploees)
            emp += emp_in_firm
            print(i.firmname.name, emp_in_firm)
        print('Общее количество сотрудников', emp)
        assert working == emp, "Количество работяг, вычисленных разными способами не совпадает!"
        assert young_people_number +  emp == people_number, "Сумма молодых и работающих не соответствует полному количеству людей!"
        print('Люди корректно разделены на молодых и работающих')




def firm_id_not_none(session:Session):
    print("=" * 30)
    with session:
        q = (select(func.count(mm.People.id).label('count'), mm.FirmName.name.label('name'))
             .join(mm.Firm, mm.People.current_firm_id == mm.Firm.id)
             .join(mm.FirmName)
             .where(mm.People.current_firm_id.is_not(None)).group_by(mm.FirmName.id))
        res = session.execute(q)
        print("Количество сторудников в фирмах:")
        for i in res:
            print(i.name, i.count)

        q = select(func.count(mm.People.id)).where(mm.People.current_firm_id.is_not(None))
        res = session.scalar(q)
        print("Общее количество сотрудников, приписанных к фирмам:", res)

def young_dnot_work(session:Session):
    print("=" * 30)
    with ((session)):
        q = (select(func.count(mm.People.id))
             .join(mm.LastSimDate, mm.LastSimDate.id == mm.LastSimDate.id) # странное условие, потому что у таблиц нет связи
             .where(func.datediff(mm.LastSimDate.date, mm.People.birth_date) < timedelta(mm.YEAR_LENGTH*20).days) # <20 лет
             .where(mm.People.current_position_id > 1) # должность выше безработного
             )
        res = session.scalar(q)
        assert res == 0 , "Ошибка! Молодые имеют работу!"
        print("Никто из молодых не работает")




def young_not_in_firm(session:Session):
    print("=" * 30)
    with ((session)):
        q = (select(func.count(mm.People.id))
             .join(mm.LastSimDate,
                   mm.LastSimDate.id == mm.LastSimDate.id)  # странное условие, потому что у таблиц нет связи
             .where(
            func.datediff(mm.LastSimDate.date, mm.People.birth_date) < timedelta(mm.YEAR_LENGTH * 20).days)  # <20 лет
             .where(mm.People.current_firm_id.is_not(None))  # закреплен за какой-нибудь фирмой
             )
        res = session.scalar(q)
        assert res == 0, "Ошибка! Молодые прикреплены к фирме!"
        print("Никто из молодых не прикреплен к фирме")


def position_quantity(session:Session):
    print("=" * 30)
    with ((session)):
        sq = select(func.max(mm.PeoplePosition.id)).group_by(mm.PeoplePosition.people_id).subquery()
        q = (select(mm.PositionNames.name.label('name'), func.count(mm.PeoplePosition.id).label('count'))
             .join(mm.PositionNames)
             .where(mm.PeoplePosition.id.in_(select(sq)))
             .group_by(mm.PeoplePosition.position_id)
             .order_by(mm.PeoplePosition.position_id.desc())
             )
        for i in session.execute(q):
            print(i.name, i.count)

def workers_has_real_firm_ids(session:Session):
    print("=" * 30)
    with ((session)):
        wq = select(func.count(mm.People.id)).where(mm.People.current_position_id > 1).scalar_subquery() # количество работающих
        sq = select(mm.Firm.id).where(mm.Firm.open_date.is_not(None)).where(mm.Firm.close_date.is_(None)).subquery()
        q = (select(func.count(mm.People.id) == wq)
             .where(mm.People.current_firm_id.in_(select(sq)))
             )
        print("Все работающие приписаны к действующим фирмам:", session.scalar(q))

def two_records_per_worker(session:Session):
    print("=" * 30)
    with ((session)):
        wq = select(mm.People).where(mm.People.current_position_id > 1).subquery()
        w = aliased(mm.People, wq)
        q = (select(mm.PeoplePosition.people_id, func.count(mm.PeoplePosition.people_id))\
             .join(w)
             .group_by(mm.PeoplePosition.people_id)
             .having(func.count(mm.PeoplePosition.people_id) != 2)
             )

        res = session.execute(q).all()
        if len(res)> 0:
            print("ОШИБКА! У всех работяг должно быть по две записи в таблице PeoplePosition")
            for i in res:
                    print(i)
        else:
            print("У работающих людей по две записи в таблице PeoplePosition. Как и должно быть.")

def each_firm_has_one_director(session:Session):
    print("=" * 30)
    with ((session)):
        q = (select(func.count(mm.People.id.distinct()) == func.count(mm.Firm.id.distinct())).join_from(mm.Firm, mm.People, mm.Firm.id == mm.People.current_firm_id)
             .where(mm.People.current_position_id == mm.POSITION_CAP))

        res = session.scalar(q)
        assert res, "Количество диркеторов от личается от количество фирм"
        print("Количество диркеторов совпадает с  количеством фирм")

def each_firm_has_one_director_differant_approach(session:Session):
    print("=" * 30)
    with ((session)):
        # посчитать количество диркеторов у кадлой фирмы
        psq = select(mm.People).where(mm.People.current_position_id == mm.POSITION_CAP).subquery()
        directors = aliased(mm.People, psq)
        q = (select(~exists())
             .join_from(mm.Firm, directors)
             .group_by(mm.Firm.id)
             .having(func.count(directors.id) != 1)
             )
        res=session.scalar(q)
        print(res)
        assert res is not True, "Не у всех фирм по одному директору"
        print("У каждой фирмы по одному директору")

def each_firm_has_one_current_director(session:Session):
    """
    Если вручную назначить в фирму второго директора, то тест покажет, что два директора не в одной фирме, а в двух
    Потому что второй директор раньше работал в качестве обычного струдника в другой фирме.
    Изначально идет отбор людей, у которых есть запись, что они являются тиректорами. Если человек раньше был
    простым работником, а потом стал директором, он уже выбран, и все записи о нем поднимаются из таблицы PeoplePosition
    Поэтому запрос ошибочно считает, что в другой фирме, где он был простым работником, тже да директора.
    """
    print("=" * 30)
    with ((session)):
        sq = (select(func.max(mm.PeoplePosition.id))
              .group_by(mm.PeoplePosition.people_id)
              .having(mm.PeoplePosition.position_id == mm.POSITION_CAP)
              ).subquery()
        psq = select(mm.PeoplePosition).where(mm.PeoplePosition.id.in_(select(sq))).subquery()
        directors = aliased(mm.PeoplePosition, psq)

        q = (select(func.count(directors.people_id) != 1)
             .join_from(directors, mm.PeopleFirm, directors.people_id == mm.PeopleFirm.people_id)
             .group_by(mm.PeopleFirm.firm_to_id)
             )

        res = session.scalars(q).all()
        print(res)
        assert not any(res), "Не у всех фирм по одному директору"
        print("У каждой фирмы по одному директору")

def last_status_qty(session:Session):
    """
    Смотрим, что из таблицы PeoplePosition правильно выбираются последние записи.
    У каждого человека имеются записи в этой таблице,
    следовательно, количество последних записей должно быть равно количеству людей
    А последние записи изначально выбираются с помощью группировки по людям.
    Для каждого уникального человека из таблицы берется запись с максимальным id.
    """
    print("=" * 30)
    with ((session)):
        lssq = (select(func.max(mm.PeoplePosition.id).label('last'))
              .group_by(mm.PeoplePosition.people_id)
              ).subquery()
        psq = select(func.count(mm.People.id)).scalar_subquery()
        q = select(func.count(lssq.c.last) == psq)
        res = session.execute(q).all()
        assert res, "Неправильно делается выборка последних записей о должносях людей"
        print("Актуальные должности людей получаются корректным способом.")




if __name__ == "__main__":
    firm_dict = create_firms()
    people = create_people(firm_dict)
    init_firms(people)
    """
    намеренно создаю ошибку, чтобы проверить, как тестовые функции на нее среагируют
    addon()
    session.commit()
    """
    '''
    
    # тесты с прошлого этапа, смысла в них немного 
    check_people(session)
    check_nobody_works(session)

    check_no_workers_in_firms(session)
    check_no_positions(session)

    '''

    # Проверка доступа к информации через отношения. Через них можно получить название фирмы и список сотрудников
    check_staff(session)
    # проверка, что все безработные устроились на работу
    # проверка, что сумма молодых и работающих равна количеству людей в симуляции
    number_of_emploees_if_firms(session)

    # получение количества сотрудников в каждой фирме через один запрос
    # получение общего количества людей, для которых идентификатор фирмы не равен None
    firm_id_not_none(session)


# количество директоров должно быть равно количеству фирм
each_firm_has_one_director(session)
each_firm_has_one_director_differant_approach(session)

each_firm_has_one_current_director(session)


# количество последних статусов (PeopleStatus) должно быть равно количеству людей
last_status_qty(session)


# никто из молодых (возраст вычислить в запросе) не должен работать и не быть приписанным к какой-либо фирме
young_dnot_work(session)
young_not_in_firm(session)

# сколько каких должностей занимают люди
position_quantity(session)

# у всех работающих должны быть айдишники действующих фирм
workers_has_real_firm_ids(session)

# у всех устроенных на работу должно быть по две записи в таблице PeoplePositions
# одна запись делается при создании людей. Всем присваивается должность безработный.
# А вторая запись возникает при устройстве на работу
two_records_per_worker(session)
