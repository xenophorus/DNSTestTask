from db import engine, session
from tables import *
from settings import settings
from pathlib import Path
from datetime import datetime
from sqlalchemy import text
import pandas as pd
from sql_requests import *
import csv


TEST_DATA = Path("../../datasets/dns_test_data")
BATCH_SIZE = int(settings.BATCH_SIZE)


def timer(func):
    """
    Measuring execution time decorator
    :param func:
    :return:
    """

    def wrapper(*args, **kwargs):
        start = datetime.now()
        result = func(*args, **kwargs)
        stop = datetime.now()
        print(f"{func.__name__} executed in {stop - start}, {args=}, {kwargs=}")
        return result
    return wrapper


def get_csv_lines(file):
    with open(file, 'r', encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if row[0]:
                yield row


def insert_data(data) -> None:
    with session() as sess:
        sess.add_all(data)
        sess.commit()


@timer
def pandas_read(csv_data):
    pd.read_csv(csv_data)


def create_tables() -> None:
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


@timer
def fill_tables() -> None:
    tables = [Cities, Branches, Products, Sales]

    for table in tables:
        csv_file = TEST_DATA.joinpath(f"t_{table.__tablename__}.csv")
        df = pd.read_csv(csv_file).drop(columns=["Unnamed: 0"])
        pd.DataFrame.to_sql(df, table.__tablename__, con=engine, index=False, chunksize=BATCH_SIZE)


@timer
def execute_request(sql_text):
    with session() as sess:
        data = sess.execute(text(sql_text))
        return data


@timer
def main() -> None:
    # Base.metadata.drop_all(bind=engine)
    # fill_tables()

    # sql_requests = [request1, request2, request3, request4, request5, request6]
    #
    # for req in sql_requests:
    #     execute_request(req)
    #

    r = '''
    with cte as (
        select
            "Период"::date as date_,
            "Номенклатура" as product_uuid,
            sum("Количество") as product_sum
        from sales
        group by date_, "Номенклатура"
        order by "Номенклатура")
    select
        cte.product_uuid,
        sum(product_sum) as total_products_sold,
        avg(product_sum) as avg_products_per_day,
        p."Ссылка"
    from cte
             join products p on p."Ссылка" = cte.product_uuid
    group by cte.product_uuid, p."Ссылка"
    order by total_products_sold desc
    ;
    '''

    data = pd.DataFrame(execute_request(r).fetchall())
    pd.DataFrame.to_sql(data, GoodsRating.__tablename__, con=engine, index=False, chunksize=BATCH_SIZE)

if __name__ == '__main__':
    main()
