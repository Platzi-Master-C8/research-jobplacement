from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Text

from base import Base

class PositionCategory(Base):
    __tablename__ = 'position_category'

    id_position_category = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(String(50), nullable=False)

    def __init__(self,
                category):

        self.category = category
