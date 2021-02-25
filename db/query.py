from psycopg2 import connect, sql



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
            print(e)
            return e


    #метод, безопасно выполняющий запросы получения данных к базе
    def select_all_execute(self, query, data=((),)):
        try:
            with connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, data)
                    return cur.fetchall()
        except Exception as e:
            print(e)
            return e




