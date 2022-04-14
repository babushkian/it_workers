from random import  randint
from datetime import date, timedelta
from names import firm_names, first_name, second_name, last_name

SIM_YEARS = .1
YEAR_LENGTH = 365

# количество фирм
COL_FIRM = len(firm_names)

DEATH_MIN_AGE = 30
DEATH_MAX_AGE = 100
DEATH_DELTA =DEATH_MAX_AGE - DEATH_MIN_AGE

RETIREMENT_MIN_AGE = 60
RETIREMENT_MAX_AGE = 80
RETIREMENT_DELTA = RETIREMENT_MAX_AGE - RETIREMENT_MIN_AGE


# возвращает индекс случайной фирмы
def get_rand_firm_id() -> int:
    return (randint(1, COL_FIRM))


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

