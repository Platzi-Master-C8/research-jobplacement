from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Text

from base import Base

class Industry(Base):
    __tablename__ = 'industry'
    id_industry = Column(Integer, primary_key=True, autoincrement=True)
    industry = Column(String(50), nullable=False)
    description = Column(Text, nullable=True)

    def __init__(self,
                industry,
                description):

        self.industry = industry
        self.description = description