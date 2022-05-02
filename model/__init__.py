from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

session=None
def bind_session(ses):
    global session
    session = ses
