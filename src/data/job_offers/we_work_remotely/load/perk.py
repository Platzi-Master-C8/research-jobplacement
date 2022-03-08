# SqlAlchemy
from sqlalchemy import Column, Integer, Text

# Base SqlAlchemy
from base import Base


class Perk(Base):
    """
    Perk table
    """
    __tablename__ = 'perk'
    id_perk = Column(Integer, primary_key=True, autoincrement=True)
    perk = Column(Text, nullable=False)

    def __init__(self, perk):
        self.perk = perk
