from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Text

from base import Base

class Location(Base):
    __tablename__ = 'location'

    id_location = Column(Integer, primary_key=True, autoincrement=True)
    country = Column(String(50), nullable=False)
    continent = Column(String(50), nullable=False)
    location_company = relationship('Company', secondary='company_location')

    def __init__(self,
                country,
                continent):

        self.country = country
        self.continent = continent
