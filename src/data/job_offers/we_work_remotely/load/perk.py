from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Text
from sqlalchemy.sql.expression import desc

from base import Base

class Perk(Base):
    __tablename__ = 'perk'
    id_perk = Column(Integer, primary_key=True, autoincrement=True)
    perk = Column(Text, nullable=False)


    def __init__(self,
                #ads_uid,
                perk):

        #self.id = ads_uid
        self.perk = perk


