# SqlAlchemy
from sqlalchemy.orm import relationship
from sqlalchemy import (
    Column,
    String,
    Integer,
    Text,
    Boolean,
    Float
)

# Base SqlAlchemy
from base import Base


class Company(Base):
    """
    Company table
    """
    __tablename__ = 'company'
    id_company = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False)
    description = Column(Text, nullable=True)
    company_premium = Column(Boolean, default=False)
    company_size = Column(Integer, nullable=True)
    ceo = Column(String(50), nullable=False)
    avg_reputation = Column(Float, nullable=True)
    total_ratings = Column(Integer, nullable=True)
    ceo_score = Column(Float, nullable=True)
    website = Column(String(150), nullable=True)
    culture_score = Column(Float, nullable=True)
    work_life_balance = Column(Float, nullable=True)
    stress_level = Column(Float, nullable=True)
    company_location = relationship('Location', secondary='company_location')

    def __init__(self,
                 name,
                 description,
                 company_premium,
                 company_size,
                 ceo,
                 avg_reputation,
                 total_ratings,
                 ceo_score,
                 website,
                 culture_score,
                 work_life_balance,
                 stress_level):
        self.name = name
        self.description = description
        self.company_premium = company_premium
        self.company_size = company_size
        self.ceo = ceo
        self.avg_reputation = avg_reputation
        self.total_ratings = total_ratings
        self.ceo_score = ceo_score
        self.website = website
        self.culture_score = culture_score
        self.work_life_balance = work_life_balance
        self.stress_level = stress_level
