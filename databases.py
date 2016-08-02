import datetime, MySQLdb, emoji, solrcloudpy

class data:
    '''
    Class implements database management. Provide method to create database, insert an object into it,
    check if it is currently empty, check if item is inside database.
    '''
    def __init__(self, configuration, channel, init = None):
        ''' Establishes database connection from given configuration file

        :param configuration: configuration file loaded from external source
        :param channel: specifies current channel name
        :param init: tells if database should be build from scratch
        '''
        self.connection = MySQLdb.connect(host=str(configuration[channel]['database']['host']),
                                          user=str(configuration[channel]['database']['user']),
                                          passwd=str(configuration[channel]['database']['password']),
                                          db=str(configuration[channel]['database']['db']),
                                          use_unicode=True,
                                          charset="utf8"
                                          )

        # self.connection_solr = solrcloudpy.SolrConnection("localhost:8983", version="6.1.0")
        # print self.connection_solr.list()

        self.channel = channel + "_pages"

        self.cursor = self.connection.cursor()

        self.cursor.execute('SET NAMES utf8mb4')
        self.cursor.execute("SET CHARACTER SET utf8mb4")
        self.cursor.execute("SET character_set_connection=utf8mb4")

        if init is not None:
            self.create_pages()
        else:
            self.cursor.execute("SHOW TABLES like \"{}\"".format(self.channel))
            result = self.cursor.fetchall()
            self.connection.commit()
            if len(result) is 0:
                print "Have to create a table cause it doesn't exists!"
                self.create_pages()
    pass

    def check_if_table_exists(self):
        '''Checks if table about given table name exists.
        :return: true if it exists, otherwise nothing.
        '''
        self.cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{0}'
            """.format(self.channel))
        if self.cursor.fetchone()[0] == 1:
            return True
    pass

    def create_pages(self):
        '''
        Creates particular table to store data from specified portal.
        '''
        drop_table = "drop table if exists %s" % self.channel
        self.cursor.execute(drop_table)

        if self.channel == 'youtube_pages':

            self.cursor.execute('''create table if not exists youtube_pages(title text, id text, etag text, description text, customUrl text,
                publishedAt text, defaultLanguage text, country text, contentDetails text, viewCount text,
                commentCount text, subscriberCount text, videoCount text, keywords text, defaultTab text, analyticsAccountId text,
                featuredChannelsTitle text, time_ text)''')
            self.cursor.execute("ALTER TABLE youtube_pages CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        elif self.channel == 'facebook_pages':

            self.cursor.execute('''create table if not exists facebook_pages(page_name longtext, id longtext, about longtext, description longtext, lang longtext,
                         link longtext, talking_about_count longtext, phone longtext, website longtext,
                         username longtext, products longtext, name_with_location_descriptor longtext,
                         mission longtext, location longtext, impressum longtext, hours longtext, general_info longtext,
                         founded longtext, fan_count longtext, display_subtext longtext, contact_address longtext,
                         company_overview longtext,category longtext, time_ text)''')
            self.cursor.execute("ALTER TABLE facebook_pages CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        elif self.channel == 'twitter_pages':

            self.cursor.execute('''create table if not exists twitter_pages(page_name longtext, screen_name longtext,
                         id longtext, verified text, description longtext, page_url longtext, location longtext, followers_count longtext,
                          friends_count longtext, listed_count longtext,
                           create_at longtext, favourites_count longtext, lang longtext,
                           statuses_count longtext, time_ text)''')
            self.cursor.execute("ALTER TABLE twitter_pages CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")

        self.connection.commit()
    pass

    def check_item(self, id):
        '''
        Checks if given item is already in database.
        :param id: specific identifier of object
        :return: such object if it is in databse, None otherwise.
        '''
        check_item = "Select * from {} where id = \"{}\"".format(self.channel, id)
        self.cursor.execute(check_item)
        check = self.cursor.fetchone()
        return check
    pass

    def insert_into(self, params, lang = None):
        '''
        Retrieves particular parameters from dictionary loaded from json file, add them to the list,
        encode if necessery, make a query to database and insert into.

        :param params: dict of parameters retrieved from json file

        '''
        if self.channel == 'youtube_pages':
            list_to_insert =self.parameters_from_youtube(params)
        elif self.channel == 'facebook_pages':
            list_to_insert= self.parameters_from_facebook(params, lang)
        elif self.channel == 'twitter_pages':
            list_to_insert = self.parameters_from_twitter(params)
        else:
            raise Exception

        for i in range(0,len(list_to_insert)):
            if list_to_insert[i] is not None:
                emotic = emoji.demojize(list_to_insert[i])
                list_to_insert[i] = list_to_insert[i].replace(list_to_insert[i],emotic)
                list_to_insert[i] = self.encode_to_string(list_to_insert[i])
                pass
            else:
                pass

        if self.channel == 'youtube_pages':
            query = self.make_query(list_to_insert, 'youtube_pages')
            self.cursor.execute(query)
        elif self.channel == 'facebook_pages':
            query = self.make_query(list_to_insert, 'facebook_pages')
            self.cursor.execute(query)
        elif self.channel == 'twitter_pages':
            query = self.make_query(list_to_insert, 'twitter_pages')
            self.cursor.execute(query)
        else:
            raise Exception

        # self.connection_solr['face_pages'].add(list_to_insert)
        self.connection.commit()
    pass

    def encode_to_string(self, element):
        '''encoding unicode variable and replacing encounter special sign

        :param element: element to be encoded and checked
        :return: element in a wanted way to be performed
        '''

        element = element.replace("\"", " ")
        element = element.replace("\\", " ")
        return element.encode("utf-8")
    pass

    def make_query(self, list, db_name):
        '''creating a query to mysql db using list object
        :param list: list of parameters to be inserted into database
        :param db_name: table name in database

        :return (string): request to database
        '''

        que = "INSERT INTO %s VALUES" % db_name
        query = que + "(%s)" % ','.join(['\"{}\"'] * len(list))
        query = query.format(*list)
        return query
    pass

    def parameters_from_youtube(self, params):
        '''Retrieves all demanded parameters from youtube.

        :param params: responsed json from api
        :return: list of parameters to be inserted into database
        '''
        import json

        list_to_insert = []
        if params['snippet']['title'].find("'") == -1:
            list_to_insert.append(params['snippet']['title'])
        else:
            name = params['snippet']['title'].replace("'", "''")
            list_to_insert.append(name)

        list_to_insert.append(params['id'])
        list_to_insert.append(params['etag'])

        if 'description' in params['snippet'].keys():
            list_to_insert.append(params['snippet']['description'])
        else:
            list_to_insert.append(None)

        if 'customUrl' in params['snippet'].keys():
            list_to_insert.append(params['snippet']['customUrl'])
        else:
            list_to_insert.append(None)

        if 'publishedAt' in params['snippet'].keys():
            list_to_insert.append(params['snippet']['publishedAt'])
        else:
            list_to_insert.append(None)

        if 'defaultLanguage' in params['snippet'].keys():
            list_to_insert.append(params['snippet']['defaultLanguage'])
        else:
            list_to_insert.append(None)

        if 'country' in params['snippet'].keys():
            list_to_insert.append(params['snippet']['country'])
        else:
            list_to_insert.append(None)

        if 'contentDetails' in params.keys():
            contentDetails = json.dumps(params['contentDetails'])
            list_to_insert.append(contentDetails)
        else:
            list_to_insert.append(None)

        if 'viewCount' in params['statistics'].keys():
            list_to_insert.append(params['statistics']['viewCount'])
        else:
            list_to_insert.append(None)

        if 'commentCount' in params['statistics'].keys():
            list_to_insert.append(params['statistics']['commentCount'])
        else:
            list_to_insert.append(None)

        if 'subscriberCount' in params['statistics'].keys():
            list_to_insert.append(params['statistics']['subscriberCount'])
        else:
            list_to_insert.append(None)

        if 'videoCount' in params['statistics'].keys():
            list_to_insert.append(params['statistics']['videoCount'])
        else:
            list_to_insert.append(None)

        if 'keywords' in params['brandingSettings']['channel'].keys():
            list_to_insert.append(params['brandingSettings']['channel']['keywords'])
        else:
            list_to_insert.append(None)

        if 'defaultTab' in params['brandingSettings']['channel'].keys():
            list_to_insert.append(params['brandingSettings']['channel']['defaultTab'])
        else:
            list_to_insert.append(None)

        if 'trackingAnalyticsAccountId' in params['brandingSettings']['channel'].keys():
            list_to_insert.append(params['brandingSettings']['channel']['trackingAnalyticsAccountId'])
        else:
            list_to_insert.append(None)

        if 'featuredChannelsTitle' in params['brandingSettings']['channel'].keys():
            list_to_insert.append(params['brandingSettings']['channel']['featuredChannelsTitle'])
        else:
            list_to_insert.append(None)

        time = str(datetime.datetime.now())
        list_to_insert.append(time)

        print "time {} {}".format(time, params['id'])
        return list_to_insert
    pass

    def parameters_from_facebook(self, params, lang):
        '''Retrieves all demanded parameters from facebook.

        :param params: responsed json from api
        :param lang: guessed language page
        :return: list of parameters to be inserted into database
        '''
        import json

        list_to_insert = []

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
            location = json.dumps(params['location'])
            list_to_insert.append(location)
        else:
            list_to_insert.append(None)

        if 'impressum' in params.keys():
            list_to_insert.append(params['impressum'])
        else:
            list_to_insert.append(None)

        if 'hours' in params.keys():
            hours = json.dumps(params['hours'])
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

        time = str(datetime.datetime.now())
        list_to_insert.append(time)

        print "time {} {}".format(time, params['id'])
        return list_to_insert
    pass

    def parameters_from_twitter(self, params):
        '''Retrieves all demanded parameters from twitter.

        :param params: responsed json from api
        :return: list of parameters to be inserted into database
        '''
        list_to_insert = []

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

        time = str(datetime.datetime.now())
        list_to_insert.append(time)

        print "time {} {}".format(time,params['id'])

        return list_to_insert
    pass