from sqlalchemy import Column, Integer, String,  Date,  ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base


from random import choice, randint, random
from datetime import date, timedelta

import settings
from settings import get_rand_firm_id, ANNO


Base = declarative_base()

class Position():
    MULTIPLIER = 365
    PROGRESSION = 2  # основание степени (тружность получения повышения растет по степенному закону в завистмости от должности)
    POSITIONS = ['стажёр', 'инженер', 'старший инженер', 'главный инженер', 'начальник отдела',
                 'начальник департамента', 'директор']
    CAP = len(POSITIONS) - 1

    def __init__(self, ses):
        self.session = ses
        self.__position = self.session.query(PosBase.id).first()[0]

    @property
    def position(self):
        return self.__position

    @property
    def posname(self):
        pos = self.session.query(PosBase.name).filter(PosBase.id == self.__position).scalar()
        return pos

    # повышение по службе
    def promotion(self, talent, experience):
        if self.__position < Position.CAP:
            x = random()
            base_mod = 1 / (Position.MULTIPLIER * Position.PROGRESSION ** self.__position)
            # умножает время перехода на следующую должность. Для умных время до повыщения сокращается
            chisl = (2 * settings.TALENT_MAX + talent)
            talent_mod = chisl / settings.TALENT_RANGE
            experience_mod = (1 + settings.EXPERIENCE_CAP * experience)
            if x < base_mod * talent_mod * experience_mod:
                self.__position += 1
                return True
            return False

    def demotion(self):  # понижние по службе
        pass


class PosBase(Base):
    __tablename__ = 'positions'
    id = Column(Integer, primary_key=True)
    name = Column(String(70))


class HumanFirm(Base):
    __tablename__ = 'human_firms'
    id = Column(Integer, primary_key=True)
    human_id = Column(Integer, ForeignKey('humans.id'))
    firm_id = Column(Integer, ForeignKey('firms.id'))
    move_to_firm_date = Column(Date, index=True)
    __table_args__ = (Index('ix_human_firms_human_id_firm_id', human_id, firm_id),)


class HumanPosition(Base):
    __tablename__ = 'human_positions'
    id = Column(Integer, primary_key=True)
    human_id = Column(Integer, ForeignKey('humans.id'))
    pos_id = Column(Integer, ForeignKey('positions.id'))
    move_to_position_date = Column(Date, index=True)
    __table_args__ = (Index('ix_human_positions_human_id_pos_id', human_id, pos_id),)


class Human(Base):
    __tablename__ = 'humans'
    id = Column(Integer, primary_key=True)
    fname = Column(String(50))
    sname = Column(String(50))
    lname = Column(String(50))
    age = Column(Date)
    talent = Column(Integer)
    srart_work = Column(Date)
    firm_id = Column(Integer, ForeignKey('firms.id'))
    pos_id = Column(Integer, ForeignKey('positions.id'))

    # дата начала работы
    # стажю. От него зависит вероятность продвижения по карьернойц лестнице
    # карьерная лестница: несколько ступеней, вероятность продвиджения на следуюшую ступень меньше, чем на предыдущую (*1.5)
    # талант: влияет на веротность повышения и на вероятность понижения
    HUM_COUNTER = 1

    def __init__(self, ses, firm_dict, firm_id):
        self.firm_dict = firm_dict
        self.session = ses
        self.fname = choice(settings.first_name)  #
        self.sname = choice(settings.second_name)  #
        self.lname = choice(settings.last_name)  #
        self.age = date(randint(1960, 1989), randint(1, 12), randint(1, 28))  # день рождения
        self.talent = randint(settings.TALENT_MIN, settings.TALENT_MAX)
        self.srart_work = self.age + timedelta(days=365 * 20)  # дата начала работы
        self.pos = Position(self.session)
        self.pos_id = self.pos.position
        self.firm = self.firm_dict[firm_id]
        self.firm_id = firm_id

    @property
    def position(self):
        return self.pos.position

    @property
    def experience(self):
        return (ANNO - self.srart_work).days

    def update(self):
        promoted = self.pos.promotion(self.talent, self.experience)
        if promoted:
            self.session.add(HumanPosition(human_id=self.id, pos_id=self.pos.position, move_to_position_date=ANNO))
            self.pos_id = self.pos.position
        tranfered = self.migrate()
        if tranfered:
            self.session.add(HumanFirm(human_id=self.id, firm_id=self.firm_id, move_to_firm_date=ANNO))

    def migrate(self):
        targ = get_rand_firm_id()
        if self.firm_id != targ:
            attraction_mod = self.firm_dict[targ].attraction - self.firm.attraction
            chanse = (40 + attraction_mod) / (40 * 365)
            if random() < chanse:
                self.firm = self.firm_dict[targ]
                self.firm_id = self.firm.id
                return True
        return False

    def __repr__(self):
        s = f'{self.lname} {self.fname} {self.sname}, {self.age}, талант:{self.talent} \
        фирма: "{self.firm.name}" долж:{self.pos.posname}, стаж: {self.experience}'
        return s


class Firm(Base):
    __tablename__ = 'firms'
    id = Column(Integer, primary_key=True)
    name = Column(String(70))

    def __init__(self, name):
        self.name = name
        self.attraction = randint(10, 40)

    def populate(self, num):
        for _ in range(num):
            self.residents.append(None)

    def update(self):
        if ANNO.day == 1 and ANNO.month == 1:
            self.attraction += randint(-4, 4)

    def __repr__(self):
        return f'<id:{self.id} "{self.name}"  престижность: {self.attraction}>'

