import yaml, twitter
import fifo_twitter, databases_twitter


class BFS_retrieving:


    '''Class implements BFS graph algorithm and its helpful method in order to collect data from twitter.

       Executed correctly class methods effectively gives huge amount of data stored in specified database.
    '''


    def __init__(self):
        '''Loads configuration file, initialise databases to store output data and queue of channel's id's
       and establish authenticated connection by given parameters in configuration file.

    '''

        config = self.load_config_file()

        self.fifo = fifo_twitter.Fifo_queue(config)  # queue of nodes left to check
        self.manage_data = databases_twitter.data(config)
        self.authentication(config)
    pass


    def load_config_file(self):
        try:
            stream = open('config/config_twitter.yml', 'r')
        except:
            try:
                stream = open('config/config_twitter.yaml', 'r')
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
            self.api = twitter.Api(consumer_key=config['Authentication']['consumer_key'],
                               consumer_secret=config['Authentication']['consumer_secret'],
                               access_token_key=config['Authentication']['access_token_key'],
                               access_token_secret=config['Authentication']['access_token_secret'],
                               sleep_on_rate_limit=True)
        except twitter.TwitterError:
            print twitter.TwitterError
    pass


    def get_followers(self, id, cursor):
        '''Retrieves 200 followers with their data and next_cursor parameter.

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
        '''Retrieve useful data about each user

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


    def check_conditions(self):
        '''Check if given user perform demanded conditions

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


    def retrieve(self, id):
        '''In twitter case retrieving is based on id's of particular account which is performing certain condition.

        Args:
            id(string): specific identifier for each account
        '''
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


def run():
    '''Basic function which do steps to retrieve all of wanted data from twitter.
       It is looping until fifo queue is empty.

       There are to options to start process:
       1. Start from verified_pages account gathers all verified accounts on twitter (id of that account)
       2. Start from previously stopped node (account id)

    '''
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
        id_verified_pages = '63796828'  # verified page twitter
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
