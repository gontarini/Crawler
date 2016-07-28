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
        self.create_pages()
    pass

    def create_pages(self):
        self.cursor.execute('''create table if not exists facebook_pages(page_name longtext, id longtext, about longtext, description longtext, lang longtext,
                     link longtext, talking_about_count longtext, phone longtext, website longtext,
                     username longtext, products longtext, name_with_location_descriptor longtext,
                     mission longtext, location longtext, impressum longtext, hours longtext, general_info longtext,
                     founded longtext, fan_count longtext, display_subtext longtext, contact_address longtext,
                     company_overview longtext,category longtext, time_ text)''')
        self.cursor.execute("ALTER TABLE facebook_pages CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        self.connection.commit()
    pass

    def check_queue(self):
        self.cursor.execute("Select * FROM queue LIMIT 1")
        self.is_queue_empty = self.cursor.fetchone()

        return self.is_queue_empty
    pass

    def check_item(self, id):
        self.cursor.execute("Select * from facebook_pages where id = %s", (id,))
        check = self.cursor.fetchone()
        return check
    pass


    def insert_into(self, params, lang):
        import json

        list_to_insert =[]

        if params['name'].find("'") == -1:
            list_to_insert.append(params['name'])
        else:
            name = params['name'].replace("'", "''")
            list_to_insert.append(name)

        list_to_insert.append(params['id'])

        if 'about' in params.keys():
            list_to_insert.append(params['about'])
        else:
            list_to_insert.append(None)

        if 'description' in params.keys():
            list_to_insert.append(params['description'])
        else:
            list_to_insert.append(None)

        list_to_insert.append(lang)

        if 'link' in params.keys():
            list_to_insert.append(params['link'])
        else:
            list_to_insert.append(None)

        if 'talking_about_count' in params.keys():
            list_to_insert.append(str(params['talking_about_count']))
        else:
            list_to_insert.append(None)

        if 'phone' in params.keys():
            list_to_insert.append(params['phone'])
        else:
            list_to_insert.append(None)

        if 'website' in params.keys():
            list_to_insert.append(params['website'])
        else:
            list_to_insert.append(None)

        if 'username' in params.keys():
            list_to_insert.append(params['username'])
        else:
            list_to_insert.append(None)

        if 'products' in params.keys():
            list_to_insert.append(params['products'])
        else:
            list_to_insert.append(None)

        if 'name_with_location_descriptor' in params.keys():
            list_to_insert.append(params['name_with_location_descriptor'])
        else:
            list_to_insert.append(None)

        if 'mission' in params.keys():
            list_to_insert.append(params['mission'])
        else:
            list_to_insert.append(None)

        if 'location' in params.keys():
            # location = json.dumps(params['location'])
            # location = str(location)
            location = str(params['location'])
            list_to_insert.append(location)
        else:
            list_to_insert.append(None)

        if 'impressum' in params.keys():
            list_to_insert.append(params['impressum'])
        else:
            list_to_insert.append(None)

        if 'hours' in params.keys():
            # hours = json.dumps(params['hours'])
            # hours = str(hours)
            hours =str(params['hours'])
            list_to_insert.append(hours)
        else:
            list_to_insert.append(None)

        if 'general_info' in params.keys():
            list_to_insert.append(params['general_info'])
        else:
            list_to_insert.append(None)

        if 'founded' in params.keys():
            list_to_insert.append(params['founded'])
        else:
            list_to_insert.append(None)

        if 'fan_count' in params.keys():
            list_to_insert.append(str(params['fan_count']))
        else:
            list_to_insert.append(None)

        if 'display_subtext' in params.keys():
            list_to_insert.append(params['display_subtext'])
        else:
            list_to_insert.append(None)

        if 'contact_address' in params.keys():
            list_to_insert.append(params['contact_address'])
        else:
            list_to_insert.append(None)

        if 'company_overview' in params.keys():
            list_to_insert.append(params['company_overview'])
        else:
            list_to_insert.append(None)

        if 'category' in params.keys():
            list_to_insert.append(params['category'])
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
        try:
            self.cursor.execute(self.make_query(list_to_insert,"facebook_pages"))
            self.connection.commit()
        except MySQLdb.MySQLError:
            print self.make_query(list_to_insert, "facebook_pages")
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
        # print query
        return query
    pass

