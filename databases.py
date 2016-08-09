import datetime, MySQLdb, emoji, solrcloudpy, solr

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

        self.channel = channel + "_pages"

        self.cursor = self.connection.cursor()

        self.cursor.execute('SET NAMES utf8mb4')
        self.cursor.execute("SET CHARACTER SET utf8mb4")
        self.cursor.execute("SET character_set_connection=utf8mb4")

        if init is not None:
            self.create_pages()
    pass

    def create_pages(self):
        '''
        Creates particular table to store data from specified portal.
        '''

        if self.channel == 'youtube_pages':

            self.cursor.execute('''create table if not exists youtube_pages(title text, id VARCHAR(50), etag text, description text, customUrl text,
                    publishedAt text, defaultLanguage text, country text, contentDetails text, viewCount text,
                    commentCount text, subscriberCount text, videoCount text, keywords text, defaultTab text, trackingAnalyticsAccountId text,
                    featuredChannelsTitle text, crawler_createdAt text, PRIMARY KEY(id))''')
            self.cursor.execute("ALTER TABLE youtube_pages CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        elif self.channel == 'facebook_pages':

            self.cursor.execute('''create table if not exists facebook_pages(page_name longtext, id VARCHAR(50), about longtext, description longtext, lang longtext,
                             link longtext, talking_about_count longtext, phone longtext, website longtext,
                             username longtext, products longtext, name_with_location_descriptor longtext,
                             mission longtext, location longtext, impressum longtext, hours longtext, general_info longtext,
                             founded longtext, fan_count longtext, display_subtext longtext, contact_address longtext,
                             company_overview longtext,category longtext, crawler_createdAt text, PRIMARY KEY(id))''')
            self.cursor.execute("ALTER TABLE facebook_pages CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        elif self.channel == 'twitter_pages':

            self.cursor.execute('''create table if not exists twitter_pages(page_name longtext, screen_name longtext,
                             id VARCHAR(50), verified text, description longtext, page_url longtext, location longtext, followers_count longtext,
                              friends_count longtext, listed_count longtext,
                               create_at longtext, favourites_count longtext, lang longtext,
                               statuses_count longtext, crawler_createdAt text, PRIMARY KEY(id))''')
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
            diction_to_insert = self.parameters_from_youtube(params)
        elif self.channel == 'facebook_pages':
            diction_to_insert= self.parameters_from_facebook(params, lang)
        elif self.channel == 'twitter_pages':
            diction_to_insert = self.parameters_from_twitter(params)
        else:
            raise Exception

        if self.channel == 'youtube_pages':
            query = """INSERT INTO youtube_pages (title, id, etag, description, customUrl, publishedAt, defaultLanguage,
            country, contentDetails, viewCount, commentCount, subscriberCount,videoCount, keywords, defaultTab, trackingAnalyticsAccountId,
             featuredChannelsTitle, crawler_createdAt) VALUES( %(title)s, %(id)s, %(etag)s, %(description)s,
             %(customUrl)s, %(publishedAt)s, %(defaultLanguage)s, %(country)s, %(contentDetails)s, %(viewCount)s,
             %(commentCount)s, %(subscriberCount)s, %(videoCount)s, %(keywords)s, %(defaultTab)s, %(trackingAnalyticsAccountId)s,
             %(featuredChannelsTitle)s, %(crawler_createdAt)s )"""
            self.cursor.execute(query, diction_to_insert)
        elif self.channel == 'facebook_pages':
            query = """INSERT INTO facebook_pages (page_name, id, about, description, lang, link, talking_about_count,
                phone, website, username, products, name_with_location_descriptor, mission, location, impressum,
                hours, general_info, founded, fan_count, display_subtext, contact_address, company_overview, category, crawler_createdAt)
                 VALUES( %(page_name)s, %(id)s, %(about)s, %(description)s,
                 %(lang)s, %(link)s, %(talking_about_count)s, %(phone)s, %(website)s, %(username)s,
                 %(products)s, %(name_with_location_descriptor)s, %(mission)s, %(location)s, %(impressum)s, %(hours)s,
                 %(general_info)s, %(founded)s, %(fan_count)s, %(display_subtext)s, %(contact_address)s, %(company_overview)s,
                  %(category)s, %(crawler_createdAt)s )"""
            self.cursor.execute(query, diction_to_insert)
        elif self.channel == 'twitter_pages':
            query = """INSERT INTO twitter_pages (page_name, screen_name, id, verified, description, page_url, location, followers_count, friends_count,
            listed_count, create_at, favourites_count, lang, statuses_count, crawler_createdAt)
                 VALUES( %(page_name)s, %(screen_name)s, %(id)s, %(verified)s, %(description)s,
                 %(page_url)s, %(location)s, %(followers_count)s, %(friends_count)s, %(listed_count)s, %(create_at)s,
                 %(favourites_count)s, %(lang)s, %(statuses_count)s, %(crawler_createdAt)s )"""
            self.cursor.execute(query, diction_to_insert)
        else:
            raise Exception

        # self.connection_solr['db'].add({'id':params['id']})
        # doc = {
        #     'id' : params['id'],
        #     'page_name' : params['name']
        # }
        # import json
        #
        # doc = json.dumps(doc)
        # print self.connection_solr['face_page'].
        # print self.connection_solr['db'].add(doc)
        self.connection.commit()
    pass

    def parameters_from_youtube(self, params):
        '''Retrieves all demanded parameters from youtube.

        :param params: responsed json from api
        :return: dictionary of parameters to be inserted into database
        '''
        import json

        diction_to_insert = {}
        if params['snippet']['title'].find("'") == -1:
            diction_to_insert['title'] = params['snippet']['title']
        else:
            name = params['snippet']['title'].replace("'", "''")
            diction_to_insert['title'] = name


        diction_to_insert['id'] = params['id']
        diction_to_insert['etag'] = params['etag']

        if 'description' in params['snippet'].keys():
            diction_to_insert['description'] = params['snippet']['description']
        else:
            diction_to_insert['description'] = ""

        if 'customUrl' in params['snippet'].keys():
            diction_to_insert['customUrl'] = params['snippet']['customUrl']
        else:
            diction_to_insert['customUrl'] = ""

        if 'publishedAt' in params['snippet'].keys():
            diction_to_insert['publishedAt'] = params['snippet']['publishedAt']
        else:
            diction_to_insert['publishedAt'] =""

        if 'defaultLanguage' in params['snippet'].keys():
            diction_to_insert['defaultLanguage'] = params['snippet']['defaultLanguage']
        else:
            diction_to_insert['defaultLanguage'] =""

        if 'country' in params['snippet'].keys():
            diction_to_insert['country'] = params['snippet']['country']
        else:
            diction_to_insert['country'] = ""

        if 'contentDetails' in params.keys():
            contentDetails = json.dumps(params['contentDetails'])
            diction_to_insert['contentDetails'] = contentDetails
        else:
            diction_to_insert['contentDetails'] = ""

        if 'viewCount' in params['statistics'].keys():
            diction_to_insert['viewCount'] = params['statistics']['viewCount']
        else:
            diction_to_insert['viewCount'] = ""

        if 'commentCount' in params['statistics'].keys():
            diction_to_insert['commentCount'] = params['statistics']['commentCount']
        else:
            diction_to_insert['commentCount'] = ""

        if 'subscriberCount' in params['statistics'].keys():
            diction_to_insert['subscriberCount'] = params['statistics']['subscriberCount']
        else:
            diction_to_insert['subscriberCount'] = ""

        if 'videoCount' in params['statistics'].keys():
            diction_to_insert['videoCount'] = params['statistics']['videoCount']
        else:
            diction_to_insert['videoCount'] = ""

        if 'keywords' in params['brandingSettings']['channel'].keys():
            diction_to_insert['keywords'] = params['brandingSettings']['channel']['keywords']
        else:
            diction_to_insert['keywords'] = ""

        if 'defaultTab' in params['brandingSettings']['channel'].keys():
            diction_to_insert['defaultTab'] = params['brandingSettings']['channel']['defaultTab']
        else:
            diction_to_insert['defaultTab'] = ""

        if 'trackingAnalyticsAccountId' in params['brandingSettings']['channel'].keys():
            diction_to_insert['trackingAnalyticsAccountId'] = params['brandingSettings']['channel']['trackingAnalyticsAccountId']
        else:
            diction_to_insert['trackingAnalyticsAccountId'] = ""

        if 'featuredChannelsTitle' in params['brandingSettings']['channel'].keys():
            diction_to_insert['featuredChannelsTitle'] = params['brandingSettings']['channel'][
                'featuredChannelsTitle']
        else:
            diction_to_insert['featuredChannelsTitle'] = ""

        time = str(datetime.datetime.now())
        diction_to_insert['crawler_createdAt']=time

        print "time {} {}".format(time, params['id'])
        return diction_to_insert
    pass

    def parameters_from_facebook(self, params, lang):
        '''Retrieves all demanded parameters from facebook.

        :param params: responsed json from api
        :param lang: guessed language page
        :return: dictionary of parameters to be inserted into database
        '''
        import json

        diction_to_insert={}
        if params['name'].find("'") == -1:
            diction_to_insert['page_name'] = params['name']
        else:
            name = params['name'].replace("'", "''")
            diction_to_insert['page_name'] = name

        diction_to_insert['id'] = params['id']

        if 'about' in params.keys():
            diction_to_insert['about'] = params['about']
        else:
            diction_to_insert['about'] = ""

        if 'description' in params.keys():
            diction_to_insert['description'] = params['description']
        else:
            diction_to_insert['description'] = ""

        diction_to_insert['lang'] = lang

        if 'link' in params.keys():
            diction_to_insert['link'] = params['link']
        else:
            diction_to_insert['link'] = ""

        if 'talking_about_count' in params.keys():
            diction_to_insert['talking_about_count'] = str(params['talking_about_count'])
        else:
            diction_to_insert['talking_about_count'] = ""

        if 'phone' in params.keys():
            diction_to_insert['phone'] = params['phone']
        else:
            diction_to_insert['phone'] =""

        if 'website' in params.keys():
            diction_to_insert['website'] = params['website']
        else:
            diction_to_insert['website']=""

        if 'username' in params.keys():
            diction_to_insert['username'] = params['username']
        else:
            diction_to_insert['username']=""

        if 'products' in params.keys():
            diction_to_insert['products'] = params['products']
        else:
            diction_to_insert['products'] = ""

        if 'name_with_location_descriptor' in params.keys():
            diction_to_insert['name_with_location_descriptor'] = params['name_with_location_descriptor']
        else:
            diction_to_insert['name_with_location_descriptor'] =""

        if 'mission' in params.keys():
            diction_to_insert['mission']=params['mission']
        else:
            diction_to_insert['mission'] = ""

        if 'location' in params.keys():
            location = json.dumps(params['location'])
            diction_to_insert['location'] = location
        else:
            diction_to_insert['location'] = ""

        if 'impressum' in params.keys():
            diction_to_insert['impressum'] = params['impressum']
        else:
            diction_to_insert['impressum'] = ""

        if 'hours' in params.keys():
            hours = json.dumps(params['hours'])
            diction_to_insert['hours'] = hours
        else:
            diction_to_insert['hours'] = ""

        if 'general_info' in params.keys():
            diction_to_insert['general_info'] = params['general_info']
        else:
            diction_to_insert['general_info']=""

        if 'founded' in params.keys():
            diction_to_insert['founded'] = params['founded']
        else:
            diction_to_insert['founded'] = ""

        if 'fan_count' in params.keys():
            diction_to_insert['fan_count'] = str(params['fan_count'])
        else:
            diction_to_insert['fan_count'] = ""

        if 'display_subtext' in params.keys():
            diction_to_insert['display_subtext'] = params['display_subtext']
        else:
            diction_to_insert['display_subtext'] =""

        if 'contact_address' in params.keys():
            diction_to_insert['contact_address'] = params['contact_address']
        else:
            diction_to_insert['contact_address'] =""

        if 'company_overview' in params.keys():
            diction_to_insert['company_overview'] = params['company_overview']
        else:
            diction_to_insert['company_overview'] = ""

        if 'category' in params.keys():
            diction_to_insert['category'] = params['category']
        else:
            diction_to_insert['category'] = ""

        time = str(datetime.datetime.now())
        diction_to_insert['crawler_createdAt'] = time

        print "time {} {}".format(time, params['id'])
        return diction_to_insert
    pass

    def parameters_from_twitter(self, params):
        '''Retrieves all demanded parameters from twitter.

        :param params: responsed json from api
        :return: dictionary of parameters to be inserted into database
        '''
        diction_to_insert = {}

        if params['name'].find("'") == -1:
            diction_to_insert['page_name'] = params['name']
        else:
            name = params['name'].replace("'", "''")
            diction_to_insert['page_name'] = name

        if params['screen_name'].find("'") == -1:
            diction_to_insert['screen_name'] = params['screen_name']
        else:
            name = params['screen_name'].replace("'", "''")
            diction_to_insert['screen_name'] = name

        diction_to_insert['id']= str(params['id'])

        if 'verified' in params.keys():
            diction_to_insert['verified'] = str(params['verified'])
        else:
            diction_to_insert['verified'] = ""

        if 'description' in params.keys():
            diction_to_insert['description'] = params['description']
        else:
            diction_to_insert['description'] = ""

        if 'status' in params.keys() and 'urls' in params['status'] and len(params['status']['urls']) is not 0:
            diction_to_insert['page_url'] = params['status']["urls"][0]['expanded_url']
        else:
            diction_to_insert['page_url'] =""

        if 'location' in params.keys():
            diction_to_insert['location'] = params['location']
        else:
            diction_to_insert['location'] = ""

        if 'followers_count' in params.keys():
            diction_to_insert['followers_count'] = str(params['followers_count'])
        else:
            diction_to_insert['followers_count'] = ""

        if 'friends_count' in params.keys():
            diction_to_insert['friends_count'] = str(params['friends_count'])
        else:
            diction_to_insert['friends_count'] = ""

        if 'listed_count' in params.keys():
            diction_to_insert['listed_count'] = str(params['listed_count'])
        else:
            diction_to_insert['listed_count'] =""

        if 'created_at' in params.keys():
            diction_to_insert['create_at'] = params['created_at']
        else:
            diction_to_insert['create_at'] = ""

        if 'favourites_count' in params.keys():
            diction_to_insert['favourites_count'] = params['favourites_count']
        else:
            diction_to_insert['favourites_count'] = ""

        if 'lang' in params.keys():
            diction_to_insert['lang'] = params['lang']
        else:
            diction_to_insert['lang'] = ""

        if 'statuses_count' in params.keys():
            diction_to_insert['statuses_count'] = str(params['statuses_count'])
        else:
            diction_to_insert['statuses_count'] = ""

        time = str(datetime.datetime.now())
        diction_to_insert['crawler_createdAt'] = time

        print "time {} {}".format(time,params['id'])

        return diction_to_insert
    pass