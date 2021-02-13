#import psycopg2
from query import query_builder
from config_db import backconfig
from psycopg2 import sql

class table(object):
    """
    Универсальный класс "Table" предназначен для определения общих методов создания и обновления таблиц
    """

    def __init__(self, query_builder):
        self.name = 'table_name'
        self.query_builder = query_builder
        self.query_fields = ''
        self.query_pk = ''
        self.query_fk = ''


    def create_table(self):
        query = sql.SQL("CREATE TABLE IF NOT EXISTS {table_name}()").format(table_name=sql.Identifier(self.name))
        return self.query_builder.create_execute(query)

    def create_fields(self):
        return self.query_builder.create_execute(self.query_fields)

    def create_pk(self):
             return self.query_builder.create_execute(self.query_pk)

    def create_fk(self):
        return self.query_builder.create_execute(self.query_fk)



class stock(table):
    """
    Таблица для храннеия информации о ценной бумаге
    """

    def __init__(self, query_builder):

        self.name = 'stock'
        self.query_builder = query_builder
        self.query_fields = """ALTER TABLE IF EXISTS stock 
                               ADD IF NOT EXISTS ticker char(5),
                               ADD IF NOT EXISTS start_date date NOT NULL,
                               ADD IF NOT EXISTS stock_type varchar NOT NULL,
                               ADD IF NOT EXISTS domicile varchar NOT NULL"""

        self.query_pk = """DO $$
                           BEGIN
                                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'stock_ticker_pk') THEN
                                    ALTER TABLE IF EXISTS stock
                                    ADD CONSTRAINT stock_ticker_pk 
                                    PRIMARY KEY (ticker);
                                END IF;
                           END;
                           $$;    
                           """
        self.query_fk = """
                        DO $$
                        BEGIN
                            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'stock_type_name_stock_stock_type_fk') THEN
                                ALTER TABLE stock
                                    ADD CONSTRAINT stock_type_name_stock_stock_type_fk
                                    FOREIGN KEY (stock_type) REFERENCES stock_type(name);
                            END IF;
                        END;
                        $$;
                        """


        self.create_table()
        self.create_fields()
        self.create_pk()

class quotes(table):
    """
    Таблица для хранения данных о котировках ценных бумах
    """

    def __init__(self, query_builder):
        self.name = 'quotes'
        self.query_builder = query_builder
        self.query_fields = """
                            ALTER TABLE IF EXISTS quotes
                            ADD IF NOT EXISTS ticker char(5),
                            ADD IF NOT EXISTS timestamp timestamp,
                            ADD IF NOT EXISTS open money,
                            ADD IF NOT EXISTS close money,
                            ADD IF NOT EXISTS volume int,
                            ADD IF NOT EXISTS currency varchar
                            """

        self.query_pk = """
                        DO $$
                        BEGIN
                            IF NOT EXISTS ( SELECT 1 FROM pg_constraint WHERE conname = 'quotes_ticker_pk') THEN
                                ALTER TABLE IF EXISTS quotes
                                    ADD CONSTRAINT quotes_ticker_pk
                                    PRIMARY KEY (ticker, timestamp);
                            END IF;
                        END;
                        $$;
                        """

        self.query_fk = """
                        DO $$
                        BEGIN
                            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'stock_ticker_quotes_ticker_fk') THEN
                                ALTER TABLE quotes
                                    ADD CONSTRAINT stock_ticker_quotes_ticker_fk
                                    FOREIGN KEY (ticker) REFERENCES stock(ticker);
                            END IF;
                            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'currency_short_name_quotes_currency_fk') THEN
                                ALTER TABLE quotes
                                    ADD CONSTRAINT currency_short_name_quotes_currency_fk
                                    FOREIGN KEY (currency) REFERENCES currency(short_name);
                            END IF;                            
                        END;
                        $$;
                        """
        self.create_table()
        self.create_fields()
        self.create_pk()

class dividends(table):
    """
    Таблица для хранения данных о дивидендах ценных бумаг
    """

    def __init__(self, query_builder):
        self.name = 'dividends'
        self.query_builder = query_builder
        self.query_fields = """
                            ALTER TABLE IF EXISTS dividends
                            ADD IF NOT EXISTS ticker char(5),
                            ADD IF NOT EXISTS timestamp timestamp,
                            ADD IF NOT EXISTS value money,
                            ADD IF NOT EXISTS currency varchar
                            """
        self.query_pk = """
                        DO $$
                        BEGIN
                            IF NOT EXISTS ( SELECT 1 FROM pg_constraint WHERE conname = 'dividends_ticker_timestamp_pk') THEN
                                ALTER TABLE IF EXISTS dividends
                                    ADD CONSTRAINT dividends_ticker_timestamp_pk
                                    PRIMARY KEY (ticker, timestamp);
                            END IF;
                        END;
                        $$;
                        """
        self.query_fk = """
                        DO $$
                        BEGIN
                            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'stock_ticker_dividends_ticker_fk') THEN
                                ALTER TABLE dividends
                                    ADD CONSTRAINT stock_ticker_dividends_ticker_fk
                                    FOREIGN KEY (ticker) REFERENCES stock(ticker);
                            END IF;
                            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'currency_short_name_dividends_currency_fk') THEN
                                ALTER TABLE dividends
                                    ADD CONSTRAINT currency_short_name_dividends_currency_fk
                                    FOREIGN KEY (currency) REFERENCES currency(short_name);
                            END IF;                            
                        END;
                        $$;
                        """
        self.create_table()
        self.create_fields()
        self.create_pk()




class stock_type(table):
    """
    Таблица для хранения данных о типах ценных бумаг
    """

    def __init__(self, query_builder):
        self.name = 'stock_type'
        self.query_builder = query_builder
        self.query_fields = """ALTER TABLE IF EXISTS stock_type
                               ADD IF NOT EXISTS name varchar,
                               ADD IF NOT EXISTS description varchar
                            """
        self.query_pk = """
                        DO $$
                        BEGIN
                            IF NOT EXISTS ( SELECT 1 FROM pg_constraint WHERE conname = 'stock_type_name_pk') THEN
                                ALTER TABLE IF EXISTS stock_type
                                    ADD CONSTRAINT stock_type_name_pk
                                    PRIMARY KEY (name);
                            END IF;
                        END;
                        $$;
                        """
        self.create_table()
        self.create_fields()
        self.create_pk()

class currency(table):
    """
    Таблица для хранения данных о валюте
    """

    def __init__(self, query_builder):
        self.name = 'currency'
        self.query_builder = query_builder
        self.query_fields = """
                            ALTER TABLE IF EXISTS currency
                            ADD IF NOT EXISTS short_name varchar,
                            ADD IF NOT EXISTS long_name varchar,
                            ADD IF NOT EXISTS country varchar
                            """
        self.query_pk = """
                        DO $$
                        BEGIN
                            IF NOT EXISTS ( SELECT 1 FROM pg_constraint WHERE conname = 'currency_short_name_pk') THEN
                                ALTER TABLE IF EXISTS currency
                                    ADD CONSTRAINT currency_short_name_pk
                                    PRIMARY KEY (short_name);
                            END IF;
                        END;
                        $$;
                        """
        self.create_table()
        self.create_fields()
        self.create_pk()


class database(object):

    def __init__(self):
        self.qb = query_builder(backconfig().dsn)
        self.stock_table = stock(self.qb)
        self.quotes_table = quotes(self.qb)
        self.stock_type_table = stock_type(self.qb)
        self.currency_table = currency(self.qb)
        self.dividends_table = dividends(self.qb)

        self.stock_table.create_fk()
        self.quotes_table.create_fk()
        self.dividends_table.create_fk()


