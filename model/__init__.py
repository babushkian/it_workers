from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from config import DB_USER, DB_PASS, DB_HOST, DB_NAME, DB_PORT

class Base(DeclarativeBase):
    pass

# engine = create_engine(f'mysql+mysqlconnector://{my_user}:{my_pass}@localhost/workers', echo= True)
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}', echo= False)
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()
