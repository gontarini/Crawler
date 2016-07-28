#!/usr/bin/python
# -*- coding: UTF-8 -*-


'''class which manage operations on database and create if such base doesnt exists.'''

import datetime, MySQLdb,emoji

class data:
    def __init__(self, configuration):
        self.connection = MySQLdb.connect(host=str(configuration['database']['host']),
                                          user=str(configuration['database']['user']),
                                          passwd=str(configuration['database']['password']),
                                          db=str(configuration['database']['db']),
                                          use_unicode=True,
                                          charset="utf8"
                                          )

        self.cursor=self.connection.cursor()
        self.cursor.execute('''drop table if exists twitter_pages''')
        self.create_pages()
    pass

    def create_pages(self):
        self.cursor.execute('''create table if not exists twitter_pages(page_name longtext, screen_name longtext,
                     id longtext, verified text, description longtext, page_url longtext, location longtext, followers_count longtext,
                      friends_count longtext, listed_count longtext,
                       create_at longtext, favourites_count longtext, lang longtext,
                       statuses_count longtext, time_ text)''')
        self.cursor.execute("ALTER TABLE twitter_pages CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        self.connection.commit()
    pass

    def check_item(self, id):
        self.cursor.execute("Select * from twitter_pages where id = %s", (id,))
        check = self.cursor.fetchone()
        return check
    pass


    def insert_into(self, params):

        list_to_insert =[]

        if params['name'].find("'") == -1:
            list_to_insert.append(params['name'])
        else:
            name = params['name'].replace("'", "''")
            list_to_insert.append(name)

        if params['screen_name'].find("'") == -1:
            list_to_insert.append(params['screen_name'])
        else:
            name = params['screen_name'].replace("'", "''")
            list_to_insert.append(name)

        list_to_insert.append(str(params['id']))
        if 'verified' in params.keys():
            list_to_insert.append(str(params['verified']))
        else:
            list_to_insert.append(None)

        if 'description' in params.keys():
            list_to_insert.append(params['description'])
        else:
            list_to_insert.append(None)

        if 'status' in params.keys() and 'urls' in params['status'] and len(params['status']['urls']) is not 0:
            list_to_insert.append(params['status']["urls"][0]['expanded_url'])
        else:
            list_to_insert.append(None)


        if 'location' in params.keys():
            list_to_insert.append(params['location'])
        else:
            list_to_insert.append(None)

        if 'followers_count' in params.keys():
            list_to_insert.append(str(params['followers_count']))
        else:
            list_to_insert.append(None)

        if 'friends_count' in params.keys():
            list_to_insert.append(str(params['friends_count']))
        else:
            list_to_insert.append(None)

        if 'listed_count' in params.keys():
            list_to_insert.append(str(params['listed_count']))
        else:
            list_to_insert.append(None)

        if 'created_at' in params.keys():
            list_to_insert.append(str(params['created_at']))
        else:
            list_to_insert.append(None)

        if 'favourites_count' in params.keys():
            list_to_insert.append(str(params['favourites_count']))
        else:
            list_to_insert.append(None)

        if 'lang' in params.keys():
            list_to_insert.append(params['lang'])
        else:
            list_to_insert.append(None)

        if 'statuses_count' in params.keys():
            list_to_insert.append(str(params['statuses_count']))
        else:
            list_to_insert.append(None)

        time =str(datetime.datetime.now())
        list_to_insert.append(time)

        for i in range(0,len(list_to_insert)):
            if list_to_insert[i] is not None:
                emotic = emoji.demojize(list_to_insert[i])
                list_to_insert[i] = list_to_insert[i].replace(list_to_insert[i], emotic)
                list_to_insert[i] = self.encode_to_string(list_to_insert[i])
                pass
            else:
                pass

        self.cursor.execute(self.make_query(list_to_insert,"twitter_pages"))
        self.connection.commit()
    pass


    def encode_to_string(self, element):
        '''encoding unicode variable and replacing encounter special sign'''

        element = element.replace("\"","'")
        element = element.replace("\\", " ")
        return  element.encode("utf-8","ignore")
    pass

    def make_query(self, list, db_name):
        '''creating a query to mysql db using list object'''

        que= "INSERT INTO %s VALUES"%db_name
        query = que + "(%s)" % ','.join(['\"{}\"'] * len(list))
        query = query.format(*list)
        return query
    pass

