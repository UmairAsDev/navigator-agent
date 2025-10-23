from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Text, DateTime, ForeignKey, Float, Table, JSON, func
)
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy import MetaData

Base = declarative_base(metadata=MetaData(schema="public"))


hts_tariff_association_table = Table(
    "hts_tariff_associations",
    Base.metadata,
    Column("hts_code_id", Integer, ForeignKey("hts_codes.id", ondelete="CASCADE"), primary_key=True),
    Column("tariff_program_id", Integer, ForeignKey("tariff_programs.id", ondelete="CASCADE"), primary_key=True),
)


class HtsCode(Base):
    __tablename__ = "hts_codes"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    hts_number = Column(String, unique=True, index=True, nullable=False)
    hts_digits = Column(String, unique=True, index=True, nullable=False)
    indent = Column(Integer, default=0, nullable=False)
    description = Column(Text, nullable=False)

    unit_of_quantity = Column(String, nullable=True)
    general_rate_of_duty = Column(String, nullable=True)
    specific_rate_of_duty = Column(String, nullable=True)
    column_2_rate_of_duty = Column(String, nullable=True)
    spec_level_1 = Column(String, nullable=False)
    spec_level_2 = Column(String, nullable=False)
    spec_level_3 = Column(String, nullable=False)
    spec_level_4 = Column(String, nullable=False)
    spec_level_5 = Column(String, nullable=False)
    spec_level_6 = Column(String, nullable=False)
    spec_level_7 = Column(String, nullable=False)
    spec_level_8 = Column(String, nullable=False)
    spec_level_9 = Column(String, nullable=False)
    spec_level_10 = Column(String, nullable=False)

    text = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

    tariff_programs = relationship(
        "TariffProgram",
        secondary=hts_tariff_association_table,
        back_populates="hts_codes",
    )


class TariffProgram(Base):
    __tablename__ = "tariff_programs"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    tariff_program = Column(String, nullable=False, index=True)
    group = Column(String, nullable=False, index=True)
    countries = Column(Text, nullable=True)  
    description = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

    hts_codes = relationship(
        "HtsCode",
        secondary=hts_tariff_association_table,
        back_populates="tariff_programs",
    )


class CountryCode(Base):
    __tablename__ = "country_codes"
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    country_name = Column(String(255), nullable=False, unique=True, index=True)
    # country_code = Column(String(255), nullable=False, unique=True, index=True)
    iso_2_code = Column(String(255), nullable=False, unique=True, index=True)
    iso_3_code = Column(String(255), nullable=False, unique=True, index=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
