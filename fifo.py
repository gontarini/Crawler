import MySQLdb

class Fifo_queue:
    '''Class which constructs fifo queue based on MySQLdB and
    give methods to operate on it like: insert, remove, check if it is empty and get an element
    '''

    def __init__(self, configuration, channel, identifier, init = None):
        ''' Establishes database connection from given configuration file

        :param configuration: configuration file loaded from external source
        :param social_media(string): string indicates queue for certain social media
        :param init: by default it is set to None. Indicates if user want to construct fifo from scratch or no.
        :param identifier: current process name
        '''

        self.connection = MySQLdb.connect(host=str(configuration[channel]['database']['host']),
                                          user=str(configuration[channel]['database']['user']),
                                          passwd=str(configuration[channel]['database']['password']),
                                          db=str(configuration[channel]['database']['db']),
                                          use_unicode = True,
                                          charset = "utf8"
                                          )
        self.identifier = str(identifier)
        self.channel = channel + "_queue"
        self.cursor = self.connection.cursor()

        table_create = "create table if not exists %s(id text, visited text)"%self.channel
        drop_table = "drop table if exists %s"%self.channel
        if init is not None:
            self.cursor.execute(drop_table)
            self.cursor.execute(table_create)
        else:
            self.cursor.execute("SHOW TABLES like \"{}\"".format(self.channel))
            result = self.cursor.fetchall()
            if len(result) is 0:
                print "Have to create a table cause it doesn't exists!"
                self.cursor.execute(table_create)
    pass

    def put(self, id):
        '''Inserts item on the last place in queue

        :param id: specific identifier of node

        '''
        insert_into = "INSERT INTO {} VALUES(\"{}\", {})".format(str(self.channel),str(id),0)
        self.cursor.execute(insert_into)
        self.connection.commit()
    pass

    def get(self):
        '''Selects first 100 element from queue.

        :return: list of max 100 parameters to create internal queue
        '''
        select_from = """SELECT * FROM {} WHERE visited="{}" LIMIT 100""".format(self.channel, self.identifier)
        self.cursor.execute(select_from)
        self.id_from_queue = self.cursor.fetchall()

        if len(self.id_from_queue) is not 0:
            self.connection.commit()
            self.update()
        else:
            self.update()
            self.cursor.execute(select_from)
            self.id_from_queue = self.cursor.fetchall()

        return self.id_from_queue
    pass

    def update(self):
        update = """UPDATE {} SET visited="{}" WHERE visited=0 LIMIT 100""".format(self.channel, self.identifier)
        self.cursor.execute(update)
        self.connection.commit()
    pass

    def remove(self,id):
        '''Removes element specified by its id.

        :param id: identifier of element

        '''
        delete_from = "DELETE FROM {} WHERE id=\"{}\"".format(self.channel, id)
        self.cursor.execute(delete_from)
        self.connection.commit()
    pass

    def is_empty(self):
        '''Checks if queue is currently empty.

        :return: true if queue is not empty, None otherwise
        '''
        is_empty = "SELECT * FROM %s LIMIT 1"%self.channel
        self.cursor.execute(is_empty)
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


