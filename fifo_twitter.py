'''class which constructs fifo queue based on database and
give methods to operate on them like: insert, remove, check if it is empty and get an element'''

import MySQLdb

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
        self.cursor.execute("drop table if exists twitter_queue")
        self.cursor.execute("create table if not exists twitter_queue(id text, starting_cursor text)")
    pass

    def put(self, id, cursor = None):
        '''the aim of the given cursor is to indicate a place to start retrieving followers in certain id, when iterator
        crossed the established line'''

        if cursor is None:
            self.cursor.execute("INSERT INTO twitter_queue VALUES(%s,%s)",(id,'0', ))
        else:
            self.cursor.execute("INSERT INTO twitter_queue VALUES(%s,%s)",(id,str(cursor), ))

        self.connection.commit()
    pass

    def if_cursor_exist(self):
        self.cursor.execute('''SELECT * FROM twitter_queue WHERE starting_cursor>0''')
        return self.cursor.fetchall()

    def get(self):
        self.cursor.execute("SELECT * FROM twitter_queue LIMIT 1")
        self.id_from_queue = self.cursor.fetchone()
        return self.id_from_queue
    pass

    def remove(self,id):
        self.cursor.execute("DELETE FROM twitter_queue WHERE id = %s",(id, ))
        self.connection.commit()
    pass

    def is_empty(self):
        self.cursor.execute("SELECT * FROM twitter_queue LIMIT 1")
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


