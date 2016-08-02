'''
Crawler application for social media.
Executable part of whole project. Giving name of social media into parameters will start for it a process.
Only one parameter is demanded, becouse only one social media can be exploring by one executing. Next parameter is optional
and it indicates if application should build database from scratch. If yes, it is necessary to add a parameter after social media
name called 'init'.

Args:
    - facebook, twitter, youtube
    - init
Example:
    $ python graph_retrieving.py facebook (without building new database)
    $ python graph_retrieving.py facebook init (with building new one)
'''

import urllib, json, yaml, twitter
import fifo,sys, Queue
import guess_language, databases


class BFS_retrieving:
    '''Class implements BFS graph algorithm and its helpful method in order to collect data from specified social_media.

       Executed correctly class methods effectively gives huge amount of data stored in specified database.
    '''

    def __init__(self, channel, init=None):
        '''Loads configuration file, initialise databases to store output data and queue of page's id's.

        :param channel: specified channel in command line arguments
        :param init: by default set to None. Parameter tells if database have to be build from scratch.
        '''

        config = self.load_config_file()

        self.channel = channel

        if init is None:
            self.manage_data = databases.data(config, self.channel)
            self.fifo = fifo.Fifo_queue(config, self.channel)
        else:
            self.manage_data = databases.data(config, self.channel, init)
            self.fifo = fifo.Fifo_queue(config, self.channel, init)

        if channel == 'facebook':
            self.set_access_token(config[channel]['access_token'])
            self.url = config[channel]['url']
        elif channel == 'youtube':
            self.app_key = config[channel]['key']
            self.url = config[channel]['url']
        elif channel == 'twitter':
            self.authentication(config)
    pass


    def load_config_file(self):

        try:
            stream = open('config/config.yml', 'r')
        except:
            try:
                stream = open('config/config.yaml', 'r')
            except IOError:
                print IOError

        return yaml.load(stream)
    pass

    def authentication(self, config):
        '''Does process of future request authentication

        Args:
            config (dict): configuration file loaded

        '''
        try:
            self.api = twitter.Api(consumer_key=config[self.channel]['Authentication']['consumer_key'],
                                   consumer_secret=config[self.channel]['Authentication']['consumer_secret'],
                                   access_token_key=config[self.channel]['Authentication']['access_token_key'],
                                   access_token_secret=config[self.channel]['Authentication']['access_token_secret'],
                                   sleep_on_rate_limit=True)
        except twitter.TwitterError:
            print twitter.TwitterError
    pass

    def get_followers(self, id, cursor):
        '''Retrieves 200 followers from twitter with their data and next_cursor parameter.

        Args:
            id (int): specific id for the account
            cursor (int): starting point to retrieve on
        Returns:
            :type tuple where index 0 indicates next cursor, index 1 previous cursor, index 2 list of 200 followers objects
    '''

        connection = True
        while (connection):
            try:
                self.followers = self.api.GetFriendsPaged(user_id=id, cursor=cursor)
                connection = False
            except OSError:
                connection = True

        return self.followers
    pass

    def get_user_lookup(self, follower):
        '''Retrieve useful data about each user from twitter.

        Args:
            follower: list of follower data
        '''

        self.user = follower.AsDict()

        if 'followers_count' in self.user.keys():
            self.followers_count = self.user["followers_count"]  ##obserwujacy
        else:
            self.followers_count = None

        if 'friends_count' in self.user.keys():
            self.friends_count = self.user["friends_count"]  ##obserwowani
        else:
            self.friends_count = None

        self.id = self.user["id"]
        if 'verified' in self.user.keys():
            self.verified = self.user['verified']
        else:
            self.verified = None

        if 'location' in self.user.keys():
            self.location = self.user['location']
        else:
            self.location = None

        if 'status' in self.user.keys() and 'urls' in self.user['status'] and len(self.user['status']['urls']) is not 0:
            self.expanded_url = self.user['status']["urls"][0]['expanded_url']
        else:
            self.expanded_url = None
    pass
    
    def get_url(self, id=None):
        '''Creates url adress to make request to facebook graph api and google api.

       Args:
          id (int, optionally): Default is None. Specific number of currently retrieving page or youtube channel

       Returns:
          url (string): url adress neccessery to make api request on.
    '''
        if self.channel == 'facebook':
            if id is None:
                url = self.url % ('sotrender', self.access_token)
                url = str(url)
                print url
                return url
            else:
                self.id = id
                url = self.url % (str(id), self.access_token)
                url = str(url)
                print url
                return url
        elif self.channel == 'youtube':
            url = self.url % (id, self.app_key)
            url = str(url)
            print url
            return url
    pass

    def set_access_token(self, access_token):
        self.access_token = access_token
    pass


    def get_json(self, url):
        '''Makes request to facebook graph api and youtube api, loads returned json to device memory.

       Args:
          url (string): adress to make request on

    '''
        data = urllib.urlopen(url)
        self.json = json.load(data)

        if self.json.get('error'):
            print 'Probably your access token is invalid, trying one more time'
            self.get_json(self.get_url(self.id))
    pass

    def check_conditions(self):
        '''Check if given user perform demanded conditions (only for twitter)

        Returns:
            bool true if conditions performed, false otherwise
        '''

        if self.verified is True:
            return True
        elif (self.followers_count > 1000 and self.expanded_url is not None and self.location is not None):
            return True
        else:
            return False
    pass

    def retrieve(self, id = None):
        '''In facebook case retrieving is based on id's of particular page.
       For each page included in data loaded from json file there are doing the following steps:
       check if that page is already in database,
       (if not) put its id into queue,
       check language of such page,
       insert into output database

       In twitter case retrieving is based on id's of particular account which is performing certain condition.

       In youtube case retrieving is based on featured channels url id's.
       For each channel included in data loaded from json file there are doing the following steps:
       check if channel is not already in database,
       (if no)
       check wether channel contains not empty 'featuredChannelsUrls' parameter,
       (if yes) add all parameter into queue, insert channel into output database
        (if no) insert channel into database

        Args:
            id(string): specific identifier for each account

    '''
        if self.channel == "facebook":
            self.retrieve_facebook()
        elif self.channel == "twitter":
            self.retrieve_twitter(id)
        elif self.channel == "youtube":
            self.retrieve_youtube()

    pass

    def retrieve_facebook(self):
        self.data = self.json['data']

        for page in self.data:
            name = page['name']
            id = page['id']

            check = self.manage_data.check_item(id)
            if check is not None:
                print "I've been here"
                break
            else:
                self.fifo.put(id)

                if 'about' in page.keys():
                    about = page['about']
                else:
                    about = None

                if 'description' in page.keys():
                    description = page['description']
                else:
                    description = None

                language = self.language_check(name, about, description)

                self.manage_data.insert_into(page, language)
    pass

    def retrieve_youtube(self):
        items = self.json['items']

        for item in items:

            check = self.manage_data.check_item(item['id'])
            if check is not None:
                print "I've been here"
                break
            else:
                if 'featuredChannelsUrls' in item['brandingSettings']['channel']:
                    featuredChannelsUrls = item['brandingSettings']['channel']['featuredChannelsUrls']
                    for url in featuredChannelsUrls:
                        self.fifo.put(url)
                    self.manage_data.insert_into(item)
                else:
                    print 'empty parameter featuredChannelsUrls'
                    self.manage_data.insert_into(item)
                    break
    pass

    def retrieve_twitter(self, id):
        next_cursor = -1
        while next_cursor is not 0:
            try:
                print "retrieving user's from twitter"
                followers_list = self.get_followers(id=id, cursor=next_cursor)
                next_cursor = followers_list[0]

                for follower in followers_list[2]:
                    self.get_user_lookup(follower)
                    check = self.manage_data.check_item(self.id)
                    if check is not None:
                        print "I've been here"
                    else:
                        if self.check_conditions():
                            # print "Conditions performed, user added to database"
                            self.fifo.put(self.id)
                            self.manage_data.insert_into(self.user)
                        else:
                            print "Condition's doesnt performed "

            except twitter.TwitterError:
                print twitter.TwitterError
    pass

    def language_check(self, name, about=None, description=None):
        '''Guessing language of each page based basically on two available parameters
       such as about and description pulled from json file. If both of them are not included,
       guessing process is operate on the page name.

       Args:
          name (string): page name
          about (string, optionally): by default set to None. Information about page
          description (string, optionally): by default set to None. Different source of information about the page

       Returns:
          lang (string): guessed language

    '''

        if about and description is not None:
            text = about + " " + description
        else:
            if description is not None:
                text = description
            else:
                if about is not None:
                    text = about
                else:
                    text = name

        try:
             lang = guess_language.guessLanguage(text)
        except IndexError:
            lang = None
        return lang
    pass

def run():
    '''Basic function which do steps to retrieve all of wanted data from facebook, twitter or youtube.
       It is looping until fifo queue is empty.

       There are to options to start process:
       FACEBOOK:
       1. Start from sotrender node (id of sotrender)
       2. Start from previously stopped node (page id stored in queue)

       YOUTUBE:
       There are to options to start process:
       1. Start from 'z dupy' channel node (id of that channel)
       2. Start from previously stopped node (channel id stored in queue)

       TWITTER:
       There are to options to start process:
       1. Start from verified_pages account gathers all verified accounts on twitter (id of that account)
       2. Start from previously stopped node (account id)

    '''
    if sys.argv.__contains__('init'):
        b = BFS_retrieving(sys.argv[1], 'init')
    else:
        b = BFS_retrieving(sys.argv[1])
    next_id = 0
    internal_queue = Queue.Queue()

    if b.fifo.is_empty():
        while b.fifo.is_empty():  ##exploring graph
            tuple_id = b.fifo.get()

            for id in tuple_id:
                internal_queue.put(id[0])

            while internal_queue.empty() is not True:
                last_id = next_id
                next_id = internal_queue.get()

                if next_id is not last_id:
                    b.fifo.remove(next_id)
                    if b.channel == "twitter":
                        b.retrieve(next_id)
                    else:
                        b.get_json(b.get_url(id=next_id))
                        b.retrieve()
                    print "current id", next_id
    else:
        if b.channel == "twitter":
            id_verified_pages = '63796828'  # verified page twitter
            b.retrieve(id_verified_pages)
        elif b.channel == "youtube":
            id = "UCNhtAn1MT600xiNiX8ErnyQ" #id of 'z dupy' channel
            b.get_json(b.get_url(id))
            b.retrieve()
        elif b.channel == "facebook":
            b.get_json(b.get_url())
            b.retrieve()

        while b.fifo.is_empty():  ##exploring graph
            tuple_id = b.fifo.get()

            for id in tuple_id:
                internal_queue.put(id[0])

            while internal_queue.empty() is not True:
                last_id = next_id
                next_id = internal_queue.get()

                if next_id is not last_id:
                    b.fifo.remove(next_id)
                    if b.channel == "twitter":
                        b.retrieve(next_id)
                    else:
                        b.get_json(b.get_url(id=next_id))
                        b.retrieve()
                    print "current id", next_id
        pass

    b.fifo.close()

if __name__ == '__main__':
    run()