import datetime
import uuid

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import ForeignKey

from db import Base


class Products(Base):
    __tablename__ = "products"

    num: Mapped[int]
    product_uuid: Mapped[uuid.UUID] = mapped_column(primary_key=True, index=True)
    product_name: Mapped[str]


class Cities(Base):
    __tablename__ = "cities"

    num: Mapped[int]
    city_uuid: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    city_name: Mapped[str]


class Branches(Base):
    __tablename__ = "branches"
    num: Mapped[int]
    branch_uuid: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    name: Mapped[str]
    city_uuid: Mapped[uuid.UUID] = mapped_column(ForeignKey("cities.city_uuid"))
    short_name: Mapped[str]
    region: Mapped[str]


class Sales(Base):
    __tablename__ = "sales"

    num: Mapped[int] = mapped_column(primary_key=True)
    date_time: Mapped[datetime.datetime]
    branch_uuid: Mapped[uuid.UUID] = mapped_column(ForeignKey("branches.branch_uuid"))
    product_uuid: Mapped[uuid.UUID] = mapped_column(ForeignKey("products.product_uuid"))
    quantity: Mapped[float]
    sale: Mapped[float]


class GoodsRating(Base):
    __tablename__ = "goods_rating"

    product_uuid: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    quantity_sold: Mapped[int]
    avg_quantity_per_day: Mapped[float]
    product_name: Mapped[str]
