from random import random, randint,  seed
from enum import Enum
from transitions import Machine
# from transitions.extensions import GraphMachine
seed(650)

class Status(int, Enum):
    UNKNOWN = 1
    YOUNG = 2
    UNEMPLOYED = 3
    EMPLOYED = 4
    VACATION = 5
    ILL = 6
    RETIRED = 7
    DEAD = 8
    COMPANY_OWNER = 9

TRANSITIONS = [
    {'trigger': 'get_young', 'source': Status.UNKNOWN, 'dest': Status.YOUNG},
    {'trigger': 'get_old', 'source': [Status.YOUNG, Status.UNKNOWN], 'dest': Status.UNEMPLOYED},
    {'trigger': 'get_job', 'source': Status.UNEMPLOYED, 'dest': Status.EMPLOYED},
    {'trigger': 'get_ill', 'source': [Status.EMPLOYED, Status.COMPANY_OWNER], 'dest': Status.ILL},
    {'trigger': 'get_vacation', 'source': [Status.EMPLOYED, Status.COMPANY_OWNER], 'dest': Status.VACATION},
    {'trigger': 'return_to_work', 'source': [Status.ILL, Status.VACATION], 'dest': Status.EMPLOYED},
    {'trigger': 'return_to_work', 'source': [Status.ILL, Status.VACATION], 'dest': Status.COMPANY_OWNER},
    {'trigger': 'get_fired', 'source': Status.EMPLOYED, 'dest': Status.UNEMPLOYED},
    {'trigger': 'retire', 'source': [Status.UNEMPLOYED, Status.EMPLOYED], 'dest': Status.RETIRED},
    {'trigger': 'die', 'source': [Status.EMPLOYED, Status.COMPANY_OWNER, Status.UNEMPLOYED, Status.RETIRED],
     'dest': Status.DEAD},
    {'trigger': 'get_CEO', 'source': [Status.EMPLOYED, Status.UNEMPLOYED], 'dest': Status.COMPANY_OWNER},
]

DEATH_MIN_AGE = 40
DEATH_DELTA = 30
RETIREMENT_MIN_AGE = 50
GET_ILL_CHANCE = 0.15
GET_VACATION_CHANCE = 0.2

class FSMStatus:
    ID = 0
    def __init__(self):
        self.id = FSMStatus.ID
        FSMStatus.ID +=1
        self.age = randint(14, 20)
        self.work_time = 0
        self.idle_time = 0
        self.ill_time = 0
        self.vacation_time = 0
        self.working_range = 0
        self.idle_range = 0
        self.ill_range = 0
        self.vacation_range = 0
        self.dead = False
        self.retired = False

        self.machine = Machine(model=self,
                               states=Status,
                               initial=Status.UNKNOWN,
                               auto_transitions=False,
                               after_state_change=self.register_transition)

        self.machine.add_transition(trigger='die',
                                    source=[Status.EMPLOYED, Status.UNEMPLOYED, Status.RETIRED],
                                    dest=Status.DEAD,
                                    conditions=self.check_death)

        self.machine.add_transition(trigger='retire',
                                    source=[Status.UNEMPLOYED, Status.EMPLOYED],
                                    dest=Status.RETIRED,
                                    conditions=self.check_retirement)

        self.machine.add_transition(trigger='get_old',
                                    source=[Status.YOUNG, Status.UNKNOWN],
                                    dest=Status.UNEMPLOYED,
                                    conditions=self.check_adult)

        self.machine.add_transition(trigger='get_young',
                                    source=Status.UNKNOWN,
                                    dest=Status.YOUNG)

        self.machine.add_transition(trigger='get_fired',
                                    source=Status.EMPLOYED,
                                    dest=Status.UNEMPLOYED,
                                    conditions=self.check_unemployed,
                                    prepare=self.employed_prepare,
                                    after=self.get_fired_after)

        self.machine.add_transition(trigger='get_job',
                                    source=Status.UNEMPLOYED,
                                    dest=Status.EMPLOYED,
                                    conditions=self.check_employed,
                                    prepare=self.unemployed_prepare,
                                    after=self.get_job_after)

        self.machine.add_transition(trigger='get_ill',
                                    source=Status.EMPLOYED,
                                    dest=Status.ILL,
                                    conditions=self.check_ill)

        self.machine.add_transition(trigger='get_vacation',
                                    source=Status.EMPLOYED,
                                    dest=Status.VACATION,
                                    conditions=self.check_vacation)


        self.machine.add_transition(trigger='back_to_work',
                                    source=[Status.ILL, Status.VACATION],
                                    dest=Status.EMPLOYED,
                                    conditions=self.check_back_to_work,
                                    prepare=self.back_to_work_prepare,
                                    after=self.back_to_work_after)


    def register_transition(self):
        print('Состояние изменилось')
    def employed_prepare(self):
        # print('Плюс год работы!!!')
        self.work_time += 1

    def unemployed_prepare(self):
        # print('Тусуюсь без работы!!!')
        self.idle_time += 1

    def back_to_work_prepare(self):
        if self.ill_range > 0:
            # print("Болею")
            self.ill_time += 1
        else:
            # print("В отпуске")
            self.vacation_time += 1


    def get_job_after(self):
        # print("У меня есть работа!")
        self.working_range = randint(2, 5)
        # print(f'Собираюсь работать {self.working_range} ходов')
        self.idle_time = 0


    def get_fired_after(self):
        # print("Я стал безработный!")
        self.idle_range = randint(1, 2)
        # print(f'Буду бездельничать {self.idle_range} ходов')
        self.work_time = 0

    def get_ill_after(self):
        print("Я заболел")

    def get_vacation_after(self):
        print("Я пошел в отпуск")

    def back_to_work_after(self):
        self.ill_range = 0
        self.ill_time = 0
        self.vacation_range = 0
        self.vacation_time = 0


    def check_adult(self):
        return self.age > 16

    def check_unemployed(self):
        return self.work_time >= self.working_range

    def check_employed(self):
        return self.idle_time >= self.idle_range

    def check_ill(self):
        if self.ill_range == 0 and random() < GET_ILL_CHANCE:
            self.ill_range = randint(1, 3)
            print(f'Болезнь на {self.ill_range} ходов')
        return self.ill_range != 0

    def check_vacation(self):
        if self.vacation_range == 0 and random() < GET_VACATION_CHANCE:
            self.vacation_range = randint(1, 3)
            print(f'Отпуск на {self.vacation_range} ходов')
        return self.vacation_range != 0

    def check_back_to_work(self):
        ill = (self.ill_range > 0) and (self.ill_time >= self.ill_range)
        vac = (self.vacation_range > 0) and (self.vacation_time >= self.vacation_range)
        return ill or vac

    def check_retirement(self):
        if self.age >= RETIREMENT_MIN_AGE:
            treshold = (self.age + 1 - RETIREMENT_MIN_AGE)/10
            if random() < treshold:
                self.retired = True
        return self.retired

    def check_death(self):

        if self.age >= DEATH_MIN_AGE:
            treshold = (self.age - DEATH_MIN_AGE)/DEATH_DELTA
            if random() < treshold: # повезло, умер
                self.dead = True
        return self.dead


    def live(self):
        self.age +=1

people = list()
for _ in range(3):
    people.append(FSMStatus())
hist = {i:[] for i in range(len(people))}
    
for anno in range(70):
    for man in people:
        if man.state == Status.DEAD:
            continue
        triggers = man.machine.get_triggers(man.state)
        # print('Возможные переходы:', triggers)
        for t in triggers:
            mt = 'may_' + t
            test_trans = getattr(man, mt)()
            # print(f'{t}: {test_trans}')
            if test_trans:
                ex = getattr(man, t)
                result = ex()
                break
        hist[man.id].append((anno, man.state.name))

        print(f"anno: {anno}, id: {man.id} age: {man.age}  work_time: {man.work_time}  idle_time: {man.idle_time}  "
              f"ill_time: {man.ill_time} vacation_time: {man.vacation_time} STATE: {man.state}")
        man.live()

for i in hist:
    print(hist[i])




