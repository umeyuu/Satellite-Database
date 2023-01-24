from sqlalchemy import Column, String
from setting import Base, ENGINE
from sqlalchemy.types import Integer, String, DateTime, Float
from datetime import datetime


class Satellite(Base):
    __tablename__ = 'satellites'
    id = Column(Integer, primary_key=True)
    sat_name = Column('sat_name', String(200))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class Charge_Sat(Base):
    __tablename__ = 'charge'
    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    lat = Column(Float)
    lon = Column(Float)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)




def main():
    Base.metadata.create_all(ENGINE)

if __name__ == "__main__":
    main()