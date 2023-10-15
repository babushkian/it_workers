from sqlalchemy import select, and_, exists, func
from sqlalchemy.orm import aliased, Session
import add_outer_dir                         # нужна для импорта модуля из внешнего каталога

import main_mysql as mm

def setup():
    mm.create_postiton_names()
    mm.create_staus_names()
    mm.create_firm_names()
    firm_dict = mm.create_all_firms(mm.INITIAL_FIRM_NUMBER)

    mm.People.obj_firms = firm_dict
    people = mm.create_people(mm.INITIAL_PEOPLE_NUMBER)
    mm.people_init(people)  # превоначальная инициация, все безработные


def check_people(session):
    print("=" * 30)
    with session:
        q = session.scalar( select(func.count(mm.People.id)) )
        print(f'количество записей о людях: {q}')
        q = select(mm.People)
        for  i in session.scalars(q):
            print(i, i.retire_date, i.death_date, i.start_work, i.current_position_id, i.current_firm_id)
        else:
            print("Записи в таблице People отсутствуют")


def check_no_positions(session:Session):
    print("=" * 30)
    with session:
        # res =  session.scalar(select(func.count(mm.People.current_position_id)))
        q = select(exists().where(mm.People.current_position_id.is_not(None)))
        res = session.scalar(q)
        print("Есть люди с присвоенными должностями", res)

def check_nobody_works(session:Session):
    print("=" * 30)
    with session:
        # нам надо убедиться что при генерации ни один человек не получил статус выше безработного
        # вообще, нужно проверить, что существуют только молодые и безработные
        # так что статус "неопределенный тоже должен отсутствовать"
        # определяем status_id для безработного
        ssq = select(mm.StatusName.id).where(mm.StatusName.name.like('работает')).scalar_subquery()

        # это такой вид скалярного подзапроса (булевый вообще-то)
        # определяем, есть ли в таблице PeopleStatus записи со значением атрибута status_id больше,
        # чем мы определили верхним поздапросом
        sq = (select(mm.PeopleStatus)
                   .join_from(mm.People, mm.PeopleStatus)
                   .join(mm.StatusName)
                   .where(mm.PeopleStatus.status_id < ssq)
                   .exists()
              )
        q = select(sq)
        # print(q)
        res = session.scalar(q)
        # довольно бесполезная проверка. Ну есть люди со статусом ниже, чем 'работает', оче мна мэто должно говорить?
        print("Есть люди со  статусом ниже, чем 'работает'", res)

        # второй запрос проверка на то, что люди не имеют статусов кроме двух разрешенных
        ssq = select(mm.StatusName.id).where(mm.StatusName.name.in_(['безработный', 'молод ещё'])).subquery()
        # print(ssq)
        sq = (select(mm.PeopleStatus)
                   .join_from(mm.People, mm.PeopleStatus)
                   .join(mm.StatusName)
                   .where(mm.PeopleStatus.status_id.not_in(select(ssq)))
                   .exists()
              )
        q = select(sq)
        res = session.scalar(q)
        print("Есть ли люди со статусом? отличным от 'безработный' и 'молод ещё'? ", res)

def check_one_status_record_per_person(session:Session):
        # проверяем, у каждого человека по одной записи в таблице PeopleStatus, это нужно сделать с помощью группировки
        # получаем количество записей в таблице на каждого человека
        sq = (select(mm.PeopleStatus.people_id.label("id"), func.count(mm.PeopleStatus.people_id).label("count"))
              .group_by(mm.PeopleStatus.people_id).subquery())
        # получаем идентификаторы людей, у которых количество записей о статусах не равно одному
        q = select(sq.c.id).where(exists().where(sq.c.count != 1))

        res = session.scalars(q).all()
        assert len(res) == 0, "Ошибка. Количество записей о статусе у пользователя не равно одной."
        print("У всех людей по одной записи о статусе")

def check_status_counts(session:Session):
    print("=" * 30)
    with session:
        # вычисляем количество людей в таблице People
        sqp = select(func.count(mm.People.id)).scalar_subquery()
        # получаем из таблицы PeopleStatus список записей с уникальными people_id
        # изначально предполагается что при инициализации на одного человека должна быть одна запись в таблице PeopleStatus
        squ = select(mm.PeopleStatus.people_id.distinct()).subquery()

        # вычисляем количество записей со статусом ниже 'работает'
        ssun = (select(func.count(mm.PeopleStatus.id))
                .where(mm.PeopleStatus.people_id.in_(squ)) # берем уникальную выборку по людям
                .where(mm.PeopleStatus.status_id < 4)
                ).scalar_subquery()
        q = select(ssun==sqp)
        res = session.scalar(q)
        print("Количество записей со статусом ниже 'работает' равно количеству людей.\n"
              "То есть у каждого человека есть запись о статусе и никто из них не работает:", res)


def check_no_workers_in_firms(session:Session):
    print("=" * 30)
    with session:
        q= select(mm.People.id).where(mm.People.recent_firm.has()) # посчитает количество
        q = select(exists().where(mm.People.recent_firm.has())) # скажет, есть люди с ассоциированными фирмами
        # этот вариант преобразуется в самый лаконичный SQL-запрос, потому что не использует джойны с другими таблицами
        q = select(exists().where(mm.People.current_firm_id.is_not(None)))

        res  = session.execute(q).scalar()
        print("Есть ли люди с местом работы (запись ссылка на фирму в таблице человека)? ", res)
# =====================================


def check_candidates_for_director(session:Session):
    """
    В самом начале программы нужно выбрать директоров фирм. Так как все в начале имеют должность безработный,
    то надо выбрать взрослых людей. Так как встречаются и люди, которым еще рано работать.
    """
    print("=" * 30)
    with session:
        q = (select(func.count(mm.People.id))
             .join(mm.PeopleStatus).join(mm.PeoplePosition)
             .where(mm.PeopleStatus.status_id == mm.Status.UNEMPLOYED.value)
             .where(mm.PeoplePosition.position_id == mm.UNEMPLOYED_POSITION)
             )
        res = session.scalar(q)
        print("Количество взрослых безработных людей: ", res)

if __name__ == "__main__":
    setup()


    # check_people(mm.session)

    # ни одному человеку не присвоена позиция (проверка позиции)
    check_no_positions(mm.session)
    # никто не работает (проверка статуса)
    check_nobody_works(mm.session)
    check_status_counts(mm.session)
    check_one_status_record_per_person(mm.session)
    # никто не работает в фирме
    check_no_workers_in_firms(mm.session)
    # ищем кандидатов в директора: трудоспособного возраса и безработных
    check_candidates_for_director(mm.session)

    # инициализировать позицию человека