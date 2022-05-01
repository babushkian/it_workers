from random import  randint
from datetime import datetime, date, timedelta
from names import firm_names, first_name, second_name, last_name, position_names

SIM_YEARS =6.3
YEAR_LENGTH = 365
INITIAL_PEOPLE_NUMBER = 100
INITIAL_FIRM_NUMBER = 6

# количество фирм
COL_FIRM = len(firm_names)

# константа для обозначения id фейковой фирмы для безработных
UNEMPLOYED = 1


DEATH_MIN_AGE = 30
DEATH_MAX_AGE = 90
DEATH_DELTA = DEATH_MAX_AGE - DEATH_MIN_AGE

OLDEST_BIRTH_DATE = datetime(1945, 1, 1)
YONGEST_BIRTH_DATE = datetime(2000, 1, 1)
BIRTH_RANGE = (YONGEST_BIRTH_DATE - OLDEST_BIRTH_DATE).days


RETIREMENT_MIN_AGE = 60
RETIREMENT_MAX_AGE = 80
RETIREMENT_DELTA = RETIREMENT_MAX_AGE - RETIREMENT_MIN_AGE

# возвращает дату рождения
def get_birthday() -> date:
    add_days = randint(0, BIRTH_RANGE)
    bd = OLDEST_BIRTH_DATE + timedelta(days=add_days)
    return bd

# дата начало симуляции

ANNO = date(2010, 1, 1)

def time_pass():
    global ANNO
    ANNO += timedelta(days=1)

def get_anno():
    return ANNO


TALENT_MIN = -3
TALENT_MAX = 3
TALENT_RANGE = TALENT_MAX - TALENT_MIN
EXPERIENCE_CAP = 1 / (YEAR_LENGTH * 40)  # через сорок лет работы шанс получить повышение удваивается

