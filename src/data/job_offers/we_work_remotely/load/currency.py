# SqlAlchemy
from sqlalchemy import Column, String, Integer

# Base SQLAlchemy
from base import Base


class Currency(Base):
    """
    Currency table
    """
    __tablename__ = 'currency'

    id_currency = Column(Integer, primary_key=True, autoincrement=True)
    currency = Column(String(50), nullable=False)
    country = Column(String(50), nullable=False)

    def __init__(self, currency, country):
        self.currency = currency
        self.country = country
