'''class which constructs fifo queue based on database and
give methods to operate on them like: insert, remove, check if it is empty and get an element'''

import  MySQLdb

class Fifo_queue:
    def __init__(self,configuration):
        self.connection = MySQLdb.connect(host=str(configuration['database']['host']),
                                          user=str(configuration['database']['user']),
                                          passwd=str(configuration['database']['password']),
                                          db=str(configuration['database']['db']),
                                          use_unicode = True,
                                          charset = "utf8"
                                          )


        self.cursor = self.connection.cursor()
        self.cursor.execute("create table if not exists facebook_queue(id text)")
    pass

    def put(self, id):
        self.cursor.execute("INSERT INTO facebook_queue VALUES(%s)",(id, ))
        self.connection.commit()
    pass

    def get(self):
        self.cursor.execute("SELECT * FROM facebook_queue LIMIT 1")
        self.id_from_queue = self.cursor.fetchone()
        return self.id_from_queue
    pass

    def remove(self,id):
        self.cursor.execute("DELETE FROM facebook_queue WHERE id = %s",(id, ))
        self.connection.commit()
    pass

    def is_empty(self):
        self.cursor.execute("SELECT * FROM facebook_queue LIMIT 1")
        result = self.cursor.fetchone()
        if result is None:
            return None
        else:
            return True
    pass

    def close(self):
        self.cursor.close()
        self.connection.close()
    pass


