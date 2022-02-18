from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Text, ForeignKey

from base import Base

class PositionSkill(Base):
    __tablename__ = 'position_skill'

    id_position_skill = Column(Integer, primary_key=True, autoincrement=True)
    position_id = Column(Integer, ForeignKey('position.id_position'), primary_key=True)
    skill_id = Column(Integer, ForeignKey('skill.id_skill'), primary_key=True)

    def __init__(self,
                position_id,
                skill_id):

        self.position_id = position_id
        self.skill_id = skill_id

