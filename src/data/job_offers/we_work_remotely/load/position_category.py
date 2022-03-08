# SqlAlchemy
from sqlalchemy import Column, String, Integer

# Base SqlAlchemy
from base import Base


class PositionCategory(Base):
    """
    PositionCategory table
    """
    __tablename__ = 'position_category'

    id_position_category = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(String(50), nullable=False)

    def __init__(self, category):
        self.category = category
