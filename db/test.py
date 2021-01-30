import psycopg2
from psycopg2 import sql
from config_db import backconfig

dsn = backconfig().dsn
name = 'stock'
fields = [['ticker', 'char(5)'], ['start_date', 'date']]
query_1 = sql.SQL('ALTER TABLE IF EXISTS {table_name}'
                 'ADD IF NOT EXISTS ticker char(5) PRIMARY KEY,'
                 'ADD IF NOT EXISTS start_date date,'
                 'ADD IF NOT EXISTS stock_type varchar(40)').format(table_name=sql.Identifier(name))

query = sql.SQL("CREATE TABLE IF NOT EXISTS {table_name}()").format(table_name=sql.Identifier(name))

query_3 = sql.SQL('ALTER TABLE IF EXISTS {table_name}'
                 'ADD IF NOT EXISTS ticker char(5),'
                 'ADD IF NOT EXISTS start_date date,'
                 'ADD IF NOT EXISTS stock_type varchar(40),'
                 'ADD IF NOT EXISTS new_column varchar(40)').format(table_name=sql.Identifier(name))


select_pk = sql.SQL('SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type '
                    'FROM   pg_index i '
                    'JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) '
                    'WHERE  i.indrelid = {table_name}::regclass AND i.indisprimary)').format(table_name=sql.Identifier(name))



def execute(query, dsn):
    try:
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
    except Exception as e:
        print(e)


def select(query, dsn):
    try:
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()
    except Exception as e:
        print(e)

execute(query, dsn)
#execute(query_1, dsn)
#execute(query_3, dsn)

print(select(select_pk, dsn))
