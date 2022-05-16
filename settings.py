from random import  randint
from datetime import datetime, date, timedelta
from names import firm_names, first_name, second_name, last_name, position_names
from copy import copy
SIM_YEARS = 5.5
YEAR_LENGTH = 365
INITIAL_PEOPLE_NUMBER = 200
INITIAL_FIRM_NUMBER = 6

# количество фирм
COL_FIRM = len(firm_names)

UNEMPLOYED_POSITION = 1

POSITION_CAP = len(position_names)

DEATH_MIN_AGE = 30
DEATH_MAX_AGE = 90
DEATH_DELTA = DEATH_MAX_AGE - DEATH_MIN_AGE

OLDEST_BIRTH_DATE = date(1945, 1, 1)
YONGEST_BIRTH_DATE = date(2000, 1, 1)
BIRTH_RANGE = (YONGEST_BIRTH_DATE - OLDEST_BIRTH_DATE).days
START_DATE = date(2005, 1, 1)

RETIREMENT_MIN_AGE = 60
RETIREMENT_MAX_AGE = 80
RETIREMENT_DELTA = RETIREMENT_MAX_AGE - RETIREMENT_MIN_AGE

# возвращает дату рождения
def get_birthday() -> date:
    add_days = randint(0, BIRTH_RANGE)
    bd = OLDEST_BIRTH_DATE + timedelta(days=add_days)
    return bd

# дата начало симуляции

ANNO = copy(START_DATE)

def time_pass():
    global ANNO
    ANNO += timedelta(days=1)

def get_anno():
    return ANNO


TRAIT_MIN = 1
TRAIT_MAX = 7
TRAIT_RANGE = TRAIT_MAX - TRAIT_MIN + 1
EXPERIENCE_CAP = 1 / (YEAR_LENGTH * 40)  # через сорок лет работы шанс получить повышение удваивается

