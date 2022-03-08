# SqlAlchemy
from sqlalchemy import Column, String, Integer, Text

# Base SqlAlchemy
from base import Base


class Industry(Base):
    """
    Industry table
    """
    __tablename__ = 'industry'
    id_industry = Column(Integer, primary_key=True, autoincrement=True)
    industry = Column(String(50), nullable=False)
    description = Column(Text, nullable=True)

    def __init__(self, industry, description):
        self.industry = industry
        self.description = description
