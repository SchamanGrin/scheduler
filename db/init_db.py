#import psycopg2
from psycopg2 import connect, sql
from config_db import backconfig


class query_builder(object):
    """
    Универсальный класс для формирования запросов
    """

    def __init__(self, dsn):
        self.dsn = dsn


    #метод, безопасно выполняющий запросы на создание объектов к базе
    def create_execute(self, query, data=((),)):
        try:
            with connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, data)
                    conn.commit()
                    return 0
        except Exception as e:
            return e


    #метод, безопасно выполняющий запросы получения данных к базе
    def select_all_execute(self, query, data=((),)):
        try:
            with connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, data)
                    return cur.fetchall()
        except Exception as e:
            return e


class table(object):
    """
    Универсальный класс "Table" предназначен для определения общих методов создания и обновления таблиц
    """

    def __init__(self, query_builder):
        self.name = 'table_name'
        self.query_builder = query_builder
        self.query_table = ''
        self.key = ''

    def create_table(self):
        query = sql.SQL("CREATE TABLE IF NOT EXISTS {table_name}()").format(table_name=sql.Identifier(self.name))
        return self.query_builder.create_execute(query)

    def create_fields(self):
        return self.query_builder.create_execute(self.query_table)

    def create_key(self):
        return self.query_builder.create_execute()



class stock(table):

    def __init__(self, query_builder):

        self.name = 'stock'
        self.query_builder = query_builder
        self.create_table()
        self.query_table=sql.SQL('ALTER TABLE IF EXISTS {TABLE_NAME} '
                                 'ADD IF NOT EXIST ticker char(5) PRIMARY KEY, '
                                 'ADD IF NOT EXIST start_date date,'
                                 'ADD IF NOT EXIST stock_type varchar')






qb = query_builder(backconfig().dsn)
stock_table = stock(qb)
r = qb.select_all_execute("SELECT table_name FROM information_schema.tables WHERE tables.table_schema='public';")


print(r)
