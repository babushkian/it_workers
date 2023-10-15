from sqlalchemy import select, and_, exists, func
from sqlalchemy.orm import aliased
import add_outer_dir



import main_mysql as mm



def check_position_names(session):
    print("=" * 30)
    with session:
        q = select(mm.PositionNames)
        for i in session.scalars(q):
            print(i.id, i.name)


def check_status_names(session):
    print("="*30)
    with session:
        q = select(mm.StatusName)
        for i in session.scalars(q):
            print(i.id, i.name)

def check_firm_names(session):
    print("=" * 30)
    with session:
        q = select(mm.FirmName)
        for i in session.scalars(q):
            print(f'{i.id:>4d} {i.name:>22s} {i.used:b}')

def check_firms(session):
    print("=" * 30)
    with session:
        q = select(mm.Firm).join_from(mm.Firm, mm.FirmName)
        print(q)
        for i in session.scalars(q):
            print(i.id, i.firmname.name, i.last_rating, i.open_date, i.close_date)

def check_firms_used_names(session):
    """
    проверка, что у незадействованных фирм в графе used стоит True
    """
    print("=" * 30)
    with session:
        ssq = select(mm.Firm.firmname_id).scalar_subquery() # список id созданных фирм
        """        
        пример исполнения подзапроса 
        ssq = select(mm.Firm).subquery() # список id созданных фирм
        ali = aliased(mm.Firm, ssq)
        b = session.scalars(select(ali.firmname_id)).all()
        print(b)
        """

        sq = select(mm.FirmName).where( mm.FirmName.id.in_(ssq) ).subquery() # получаем имена созданных фирм
        q = select(exists().where(sq.c.used == True)) # эти имена отмечены как использованные
        print(q)
        assert session.scalar(q), "Есть действующие фирмы, имена которых не отмечены, как занятые "
        sq = select(mm.FirmName).where(mm.FirmName.id.not_in(ssq) ).subquery() # получаем имена созданных фирм
        q = select(exists().where(sq.c.used == False)) # эти имена отмечены как использованные
        print(q)
        assert session.scalar(q) is True, "Есть не занятые под фирмы имена,отмеченные как используемые "
        print('Имена присвоены к действующим фирмам правильно')


def get_firm_object(session:mm.session):
    # добываем обьект по первичному ключу
    print("=" * 30)
    with session:
        sq = select(mm.FirmName.id).order_by(mm.FirmName.id.desc()).limit(1)
        b = session.scalar(sq)
        print(b)
        # в качестве первочного ключа не допускаются SQL-выражения. id нужно определять в другом запросе
        a = session.get(mm.Firm, b)
        print(f"использовали метод get с id {b} получили {a}")
        # попытка получить фирму по первой записи в таблице с именами фирм (с использованием подзапроса)
        sq = select(func.min(mm.FirmName.id)).scalar_subquery()
        s = select(mm.Firm.id).where(mm.Firm.firmname_id == sq)
        print(s)
        q = session.scalar(s)
        print("max id", q)

def check_people(session):
    print("=" * 30)
    with session:
        q = session.scalar( select(func.count(mm.People.id)) )
        print(f'количество записей о людях: {q}')
        q = select(mm.People)
        for  i in session.scalars(q):
            print(i)
        else:
            print("Записи в таблице People отсутствуют")


check_position_names(mm.session)
check_status_names(mm.session)
check_firm_names(mm.session)
check_firms(mm.session)
check_firms_used_names(mm.session)
get_firm_object(mm.session)
check_people(mm.session)