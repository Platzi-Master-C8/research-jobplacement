# SqlAlchemy
from sqlalchemy import Column, String, Integer

# Base SqlAlchemy
from base import Base


class Seniority(Base):
    """
    Seniority table
    """
    __tablename__ = 'seniority'

    id_seniority = Column(Integer, primary_key=True, autoincrement=True)
    seniority = Column(String(50), nullable=False)

    def __init__(self, seniority):
        self.seniority = seniority
