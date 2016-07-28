import yaml, twitter, pprint
import fifo_twitter,databases_twitter
import guess_language

class BFS_retrieving:

    def __init__(self):
        '''Initialise database and loading a config file,
        establish authenticated connection'''

        config = self.load_config_file()

        self.fifo = fifo_twitter.Fifo_queue(config)  #queue of nodes left to check
        self.manage_data = databases_twitter.data(config)
        self.authentication(config)
    pass

    def load_config_file(self):
        try:
            stream = open('config/config_twitter.yml', 'r')
        except:
            try:
                stream = open('config/config_twitter.yaml','r')
            except IOError:
                print IOError

        return yaml.load(stream)
    pass

    def authentication(self, config):
        try:
            self.api = twitter.Api(consumer_key=config['Authentication']['consumer_key'],
                              consumer_secret=config['Authentication']['consumer_secret'],
                              access_token_key=config['Authentication']['access_token_key'],
                              access_token_secret=config['Authentication']['access_token_secret'],
                              sleep_on_rate_limit=True)
        except twitter.TwitterError:
            print twitter.TwitterError
    pass

    def get_followers(self, id, cursor):
        '''retrieve 200 followers with their data and next_cursor parameter,
        :return value is a tuple where index 0 indicates next cursor, index 1 previous cursor, index 2 list of 200 followers objects'''

        self.followers = self.api.GetFriendsPaged(user_id=id,cursor=cursor)
        return self.followers
    pass

    def get_user_lookup(self, follower):
        '''retrieve useful data about each user'''

        self.user = follower.AsDict()

        if 'followers_count' in self.user.keys():
            self.followers_count = self.user["followers_count"] ##obserwujacy
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

    def check_conditions(self):
        '''check if given user perform demanded conditions'''

        if self.verified is True:
            return True
        elif (self.followers_count > 1000 and self.expanded_url is not None and self.location is not None):
            return True
        else:
            return False
    pass

    def retrieve(self, id, cursor = None):
        '''In twitter case retrieving is based on id's of particular page which is performing certain condition.
            In order to retrieve all pages from portal it is used BFS algorithm,
            cursor tells if the previous retrieving was stopped and handled that events'''
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
                            print "Conditions performed, user added to database"
                            self.fifo.put(self.id)
                            self.manage_data.insert_into(self.user)
                        else:
                            print "Condition's doesnt performed "

            except twitter.TwitterError:
                print twitter.TwitterError
    pass

    def language_check(self, name, about=None, description=None):
        '''Guessing language of each page based on two available parameters
        such as about and description pulled from json file'''

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

        if len(text) >= 2:
            lang = guess_language.guessLanguage(text)
        else:
            lang = None
        return lang



def run():
    next_id = 0
    if b.fifo.is_empty():
        while b.fifo.is_empty():  ##exploring graph
            last_id = next_id
            next_id = b.fifo.get()[0]
            if next_id is not last_id:
                b.fifo.remove(next_id)
                b.retrieve(next_id)
                print "current id", next_id
    else:
        id_verified_pages = '63796828' #verified page twitter
        b.retrieve(id_verified_pages)

        while b.fifo.is_empty():  ##exploring graph
            last_id = next_id
            next_id = b.fifo.get()[0]
            if next_id is not last_id:
                b.fifo.remove(next_id)
                b.retrieve(next_id)
                print "current id", next_id
pass

if __name__ == '__main__':
    b = BFS_retrieving()
    run()
