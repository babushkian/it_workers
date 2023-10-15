from random import  randint
from datetime import datetime, date, timedelta
from names import firm_names, first_name, second_name, last_name, position_names
from copy import copy

import random
random.seed(667)

SIM_YEARS = 15.0  # длительность симуляции в годах
YEAR_LENGTH = 365
INITIAL_PEOPLE_NUMBER = 200  # сколько людей в симуляции
INITIAL_FIRM_NUMBER = 4  # сколько фирм в симуляции
FIRM_MIGRATION_POOL = INITIAL_FIRM_NUMBER-2
assert FIRM_MIGRATION_POOL > 0, "Слишком мало фирм, некуда переходить в случае смены работы"
MAX_FIRMS_QTY = min(max(4, INITIAL_PEOPLE_NUMBER//12), 20)
# количество доступных фирм (считаем по количеству названий)
COL_FIRM = len(firm_names)

UNEMPLOYED_POSITION = 1

POSITION_CAP = len(position_names)


TRAIT_MIN = 1
TRAIT_MAX = 7
TRAIT_RANGE = TRAIT_MAX - TRAIT_MIN + 1


DEATH_MIN_AGE = 30 # когда люди начинают умирать
DEATH_MAX_AGE = 90 # когда они должны быть мертвыми по идее
DEATH_DELTA = DEATH_MAX_AGE - DEATH_MIN_AGE

OLDEST_BIRTH_DATE = date(1945, 1, 1)
YONGEST_BIRTH_DATE = date(2000, 1, 1)
BIRTH_RANGE = (YONGEST_BIRTH_DATE - OLDEST_BIRTH_DATE).days
START_DATE = date(2005, 1, 1)

RETIREMENT_MIN_AGE = 60
RETIREMENT_MAX_AGE = 80
RETIREMENT_DELTA = RETIREMENT_MAX_AGE - RETIREMENT_MIN_AGE

ILL_BASE_CHANCE = 3/YEAR_LENGTH
VACATION_CHANCE = 1/YEAR_LENGTH


def get_birthday() -> date:
    """возвращает дату рождения"""
    add_days = randint(0, BIRTH_RANGE)
    bd = OLDEST_BIRTH_DATE + timedelta(days=add_days)
    return bd

# дата начало симуляции
ANNO = copy(START_DATE)

def time_pass():
    global ANNO
    ANNO += timedelta(days=1)

def get_anno():
    """Возвращает текущую дату datetime.date"""
    return ANNO

def firm_creat_probability(firm_qty: int)-> bool:
    """
    Первые три фирмы созлются с шансом 100%
    Шанс создания каждой следующей убывает в два раза
    Нужно смотреть, чстобы она правильно обрабатывала закртые фирмы, а то если неправильно получать количество фирм,
    старые фтрмы будут закрываться, а вероятность открытия новых будет становиться все меньше
    """
    x = 2 ** (4 - firm_qty)
    print('Вероятность создать фирму:', firm_qty, x)
    return random.random() < x


EXPERIENCE_CAP = 1 / (YEAR_LENGTH * 40)  # через сорок лет работы шанс получить повышение удваивается

