from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, ForeignKey

from base import Base

class CompanyLocation(Base):
    __tablename__ = 'company_location'

    id_company_location = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('company.id_company'), primary_key=True)
    location_id = Column(Integer, ForeignKey('location.id_location'), primary_key=True)

    def __init__(self,
                company_id,
                location_id):

        self.company_id = company_id
        self.location_id = location_id