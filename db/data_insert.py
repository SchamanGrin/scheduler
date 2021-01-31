import psycopg2 as ps2
import json

data_1 = [
    {'name':'Ivan', 'hobbies':json.dumps({'sports': ['футбол', 'плавание'], 'home_lib': True, 'trips': 3})},
    {'name':'Petr', 'hobbies':json.dumps({'sports': ['теннис', 'плавание'], 'home_lib': True, 'trips':2})},
    {'name':'Pavel', 'hobbies':json.dumps({'sports': ['плавание'], 'home_lib': False, 'trips':4})},
    {'name':'Boris', 'hobbies':json.dumps({'sports': ['футбол', 'плавание', 'теннис'], 'home_lib': True, 'trips':0})}
    ]

data = {'param': json.dumps({'spots':['футбол']})}

def insert_sql(data, conn):
    with conn.cursor() as cur:
        query = 'INSERT INTO pilot_hobbies (pilot_name, hobbies) VALUES (%(name)s, %(hobbies)s);'
        cur.executemany(query, data)
        print(f'')

def select_sql(data, conn):
    with conn.cursor() as cur:
        query = 'SELECT * FROM pilot_hobbies' \
                'WHERE hobbies @> %(param)s'
        return cur.execute(query, data)


with ps2.connect('dbname=demo user=db_user') as conn:
    t = select_sql(data, conn)
    print(t)