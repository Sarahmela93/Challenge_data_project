from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class StreetLightFault(Base):
    __tablename__ = "street_light_faults"

    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(100))
    location = Column(String(255))
    fault_type = Column(String(100))
    fault_status = Column(String(50))
    latitude = Column(Float)
    longitude = Column(Float)
    report_date = Column(String(50)) 		
