from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Text

from base import Base

class Skill(Base):
    __tablename__ = 'skill'

    id_skill = Column(Integer, primary_key=True, autoincrement=True)
    skill = Column(String(150), nullable=False)
    skill_position = relationship('Position', secondary='position_skill')

    def __init__(self,
                skill):

        self.skill = skill