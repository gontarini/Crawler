import MySQLdb

class Fifo_queue:
    '''Class which constructs fifo queue based on MySQLdB and
    give methods to operate on it like: insert, remove, check if it is empty and get an element'''

    def __init__(self,configuration):
        ''' Establishes database connection from given configuration file

        :param configuration: configuration file loaded from external source
        '''
        self.connection = MySQLdb.connect(host=str(configuration['database']['host']),
                                          user=str(configuration['database']['user']),
                                          passwd=str(configuration['database']['password']),
                                          db=str(configuration['database']['db']),
                                          use_unicode = True,
                                          charset = "utf8"
                                          )

       # self.cursor.execute("drop table if exists queue")
        self.cursor = self.connection.cursor()

        self.cursor.execute("create table if not exists youtube_queue(id text)")
    pass

    def put(self, id):
        '''Inserts item on the last place in queue

        :param id: specific identifier of node

        '''
        self.cursor.execute("INSERT INTO youtube_queue VALUES(%s)",(id, ))
        self.connection.commit()
    pass

    def get(self):
        '''Selects first element from queue.

        :return: first parameter from queue
        '''
        self.cursor.execute("SELECT * FROM youtube_queue LIMIT 1")
        self.id_from_queue = self.cursor.fetchone()
        return self.id_from_queue
    pass

    def remove(self,id):
        '''Removes element specified by its id.

        :param id: identifier of element

        '''
        self.cursor.execute("DELETE FROM youtube_queue WHERE id = %s",(id, ))
        self.connection.commit()
    pass

    def is_empty(self):
        '''Checks if queue is currently empty.

        :return: true if queue is not empty, None otherwise
        '''
        self.cursor.execute("SELECT * FROM youtube_queue LIMIT 1")
        result = self.cursor.fetchone()
        if result is None:
            return None
        else:
            return True
    pass

    def close(self):
        '''Closes connection with database.

        '''
        self.cursor.close()
        self.connection.close()
    pass


