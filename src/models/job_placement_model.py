# SqlAlchemy
from sqlalchemy.orm import declarative_base
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Float,
    ForeignKey,
    Text,
    Boolean,
)
from sqlalchemy.orm import relationship

# Declarative base
Base = declarative_base()


class Skill(Base):
    """
    Skill model for the skills table.

    This table contains the skills that are used in the job_placement table.
    """
    __tablename__ = 'skill'

    id_skill = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    skill = Column(String(150), nullable=False)
    skill_position = relationship('Position', secondary='position_skill')


class Seniority(Base):
    """
    Seniority model for the seniority table.

    This table contains the seniority levels that are used in the job_placement table.
    """
    __tablename__ = 'seniority'

    id_seniority = Column(Integer, primary_key=True, autoincrement=True)
    seniority = Column(String(50), nullable=False)


class Currency(Base):
    """
    Currency model for the currency table.

    This table contains the currencies that are used in the job_placement table.
    """
    __tablename__ = 'currency'

    id_currency = Column(Integer, primary_key=True, autoincrement=True)
    currency = Column(String(50), nullable=False)
    country = Column(String(50), nullable=False)


class PositionCategory(Base):
    """
    PositionCategory model for the position_category table.

    This table contains the position categories that are used in the job_placement table.
    """
    __tablename__ = 'position_category'

    id_position_category = Column(Integer, primary_key=True, autoincrement=True)
    category = Column(String(50), nullable=False)


class Position(Base):
    """
    Position model for the position table.

    This table contains the job positions that are used in the job_placement table.
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
    uid = Column(Text, nullable=False)
    position_company = relationship('Company')
    position_skill = relationship('Skill', secondary='position_skill')


class PositionSkill(Base):
    """
    PositionSkill model for the position_skill table.

    This table contains the skills that are used in the job_placement table.
    """
    __tablename__ = 'position_skill'

    id_position_skill = Column(Integer, primary_key=True, autoincrement=True)
    position_id = Column(Integer, ForeignKey('position.id_position'), primary_key=True)
    skill_id = Column(Integer, ForeignKey('skill.id_skill'), primary_key=True)


class CompanyPerk(Base):
    """
    CompanyPerk model for the company_perk table.

    This table contains the perks that are used in the job_placement table.
    """
    __tablename__ = 'company_perk'

    id_position_perk = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('company.id_company'), primary_key=True)
    perk_id = Column(Integer, ForeignKey('perk.id_perk'), primary_key=True)


class Perk(Base):
    """
    Perk model for the perk table.

    This table contains the perks that are used in the job_placement table.
    """
    __tablename__ = 'perk'

    id_perk = Column(Integer, primary_key=True, autoincrement=True)
    perk = Column(String(150), nullable=False, unique=True)
    perk_company = relationship('Company', secondary='company_perk')


class Company(Base):
    """
    Company model for the company table.

    This table contains the companies that are used in the job_placement table.
    """
    __tablename__ = 'company'

    id_company = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), nullable=False, unique=True)
    description = Column(Text, nullable=False)
    company_premium = Column(Boolean, default=False)
    company_size = Column(Integer, nullable=True)
    ceo = Column(String(50), nullable=True)
    avg_reputation = Column(Float(2), nullable=True)
    total_ratings = Column(Integer, nullable=True)
    death_line = Column(DateTime, nullable=True)
    ceo_score = Column(Float(2), nullable=True)
    website = Column(Text, nullable=True)
    culture_score = Column(Float(2), nullable=True)
    work_life_balance = Column(Float(2), nullable=True)
    stress_level = Column(Float(2), nullable=True)
    carrer_opportunities = Column(Float(2), nullable=True)
    company_location = relationship('Location', secondary='company_location')
    company_perk = relationship('Perk', secondary='company_perk')


class CompanyLocation(Base):
    """
    CompanyLocation model for the company_location table.

    This table contains the locations that are used in the company table.
    """
    __tablename__ = 'company_location'

    id_company_location = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('company.id_company'), primary_key=True)
    location_id = Column(Integer, ForeignKey('location.id_location'), primary_key=True)


class Location(Base):
    """
    Location model for the location table.

    This table contains the locations that are used in the job_placement table.
    """
    __tablename__ = 'location'

    id_location = Column(Integer, primary_key=True, autoincrement=True)
    country = Column(String(150), nullable=False)
    continent = Column(String(50), nullable=False)
    location_company = relationship('Company', secondary='company_location')


class UserReview(Base):
    """
    UserReview model for the user_review table.

    This table contains the reviews that are used in the job_placement table.
    """
    __tablename__ = 'user_review'

    id_review = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('company.id_company'))
    review_company = relationship('Company')
    review_title = Column(String(150), nullable=True)
    job_location = Column(String(150), nullable=True)
    is_still_working_here = Column(Boolean, nullable=True)
    position_user = Column(String(50), nullable=True)
    content_type = Column(Text, nullable=True)
    review_score = Column(Float(2), nullable=False)
    review_date = Column(DateTime, nullable=False)
    utility_counter = Column(Integer, nullable=True, default=0)
    score_work_environment = Column(Float(2), nullable=True)
    score_career_development = Column(Float(2), nullable=True)
    score_culture = Column(Float(2), nullable=True)
    score_perks = Column(Float(2), nullable=True)
    score_stress_level = Column(Float(2), nullable=True)
    score_work_life_balance = Column(Float(2), nullable=True)
    diversity_score = Column(Float(2), nullable=True)
    review_link = Column(Float(2), nullable=False)
