from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Text

from base import Base

class Seniority(Base):
    __tablename__ = 'seniority'

    id_seniority = Column(Integer, primary_key=True, autoincrement=True)
    seniority = Column(String(50), nullable=False)

    def __init__(self,
                seniority):

        self.seniority = seniority