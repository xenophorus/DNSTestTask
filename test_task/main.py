from db import engine, session
from tables import *
from settings import settings
from pathlib import Path
from datetime import datetime
from sqlalchemy import text
from sql_requests import *
import csv


TEST_DATA = Path("../../datasets/dns_test_data")

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
def fill_table(table, data=None) -> None:
    """
    Function for filling tables with csv data
    :param data:
    :param table:
    :return: None
    """
    data_list = []
    columns = Base.metadata.tables.get(table.__tablename__).columns.keys()
    batch_size = int(settings.get_batch_size())
    if data:
        source = data
    else:
        csv_file = TEST_DATA.joinpath(f"t_{table.__tablename__}.csv")
        source = get_csv_lines(csv_file)
    for line in source:
        dict_line = table(**dict(zip(columns, line)))
        data_list.append(dict_line)
        if len(data_list) == batch_size:
            insert_data(data_list)
            data_list.clear()
    if len(data_list) > 0:
        insert_data(data_list)


def create_tables() -> None:
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def fill_tables() -> None:
    tables = [Cities, Branches, Products, Sales]
    for table in tables:
        fill_table(table)


@timer
def execute_request(sql_text):
    with session() as sess:
        data = sess.execute(text(sql_text))
        return data


@timer
def main() -> None:
    create_tables()

    fill_tables()

    sql_requests = [request1, request2, request3, request4, request5, request6]

    for req in sql_requests:
        execute_request(req)

    fill_table(GoodsRating, execute_request(request4))


if __name__ == '__main__':
    main()
