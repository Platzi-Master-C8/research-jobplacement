from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Text, ForeignKey

from base import Base

class CompanyPerk(Base):
    __tablename__ = 'company_perk'

    id_position_perk = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('company.id_company'), primary_key=True)
    perk_id = Column(Integer, ForeignKey('perk.id_perk'), primary_key=True)

    def __init__(self,
                company_id,
                perk_id):

        self.company_id = company_id
        self.perk_id = perk_id

