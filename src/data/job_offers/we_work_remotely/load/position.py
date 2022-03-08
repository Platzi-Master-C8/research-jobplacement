# SqlAlchemy
from sqlalchemy.orm import relationship
from sqlalchemy import (
    Column,
    String,
    Integer,
    DateTime,
    Boolean,
    Text,
    ForeignKey
)

# Base SqlAlchemy
from base import Base


class Position(Base):
    """
    Position table
    """
    __tablename__ = 'position'

    id_position = Column(Integer, primary_key=True, autoincrement=True)
    position_title = Column(String(150), nullable=False)
    position_category_id = Column(Integer, ForeignKey('position_category.id_position_category'))
    position_category = relationship('PositionCategory')
    seniority_id = Column(Integer, ForeignKey('seniority.id_seniority'))
    seniority = relationship('Seniority')
    description = Column(Text, nullable=False)
    modality = Column(String(50), nullable=False)
    date_position = Column(DateTime, nullable=False)
    activate = Column(Boolean, default=True, nullable=False)
    num_offers = Column(Integer, default=1, nullable=True)
    salary_min = Column(Integer, nullable=True)
    salary_max = Column(Integer, nullable=True)
    salary = Column(Integer, nullable=True)
    currency_id = Column(Integer, ForeignKey('currency.id_currency'))
    currency = relationship('Currency')
    remote = Column(Boolean, nullable=False)
    location_id = Column(Integer, ForeignKey('location.id_location'))
    position_location = relationship('Location')
    english = Column(Boolean, nullable=False)
    english_level = Column(String(50), nullable=True)
    position_url = Column(Text, nullable=False)
    company_id = Column(Integer, ForeignKey('company.id_company'))
    position_company = relationship('Company')
    position_skill = relationship('Skill', secondary='position_skill')
    uid = Column(Text, nullable=True)

    def __init__(self,
                 position_title,
                 position_category_id,
                 seniority_id,
                 description,
                 modality,
                 date_position,
                 activate,
                 num_offers,
                 salary_min,
                 salary_max,
                 salary,
                 currency_id,
                 remote,
                 location_id,
                 english,
                 english_level,
                 position_url,
                 company_id,
                 uid):
        self.position_title = position_title
        self.position_category_id = position_category_id
        self.seniority_id = seniority_id
        self.description = description
        self.modality = modality
        self.date_position = date_position
        self.activate = activate
        self.num_offers = num_offers
        self.salary_min = salary_min
        self.salary_max = salary_max
        self.salary = salary
        self.currency_id = currency_id
        self.remote = remote
        self.location_id = location_id
        self.english = english
        self.english_level = english_level
        self.position_url = position_url
        self.company_id = company_id
        self.uid = uid
