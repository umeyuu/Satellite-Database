from sqlalchemy import Column, String, ForeignKey
from setting import Base, ENGINE
from sqlalchemy.types import Integer, String, DateTime, Float
from datetime import datetime


class Satellite(Base):
    __tablename__ = 'satellites'
    id = Column(Integer, primary_key=True)
    sat_name = Column('sat_name', String(200))
    created_at = Column(DateTime, default=datetime.now)

class Charge_Sat(Base):
    __tablename__ = 'charge'
    id = Column(Integer, primary_key=True)
    satellite_id = Column(Integer)
    date = Column(DateTime)
    lat = Column(Float)
    lon = Column(Float)
    charge_count = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)


def main():
    Base.metadata.create_all(ENGINE)

if __name__ == "__main__":
    main()