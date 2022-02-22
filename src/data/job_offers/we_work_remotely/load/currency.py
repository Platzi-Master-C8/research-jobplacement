from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Text

from base import Base

class Currency(Base):
    __tablename__ = 'currency'

    id_currency = Column(Integer, primary_key=True, autoincrement=True)
    currency = Column(String(50), nullable=False)
    country = Column(String(50), nullable=False)

    def __init__(self,
                currency,
                country):

        self.currency = currency
        self.country = country