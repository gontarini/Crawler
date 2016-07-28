import urllib, json, yaml
import fifo_youtube, databases_youtube
import guess_language


class BFS_retrieving:
    '''Class implements BFS graph algorithm and its helpful method in order to collect data from youtube.
       Executed correctly class methods effectively gives huge amount of data stored in specified database.
    '''

    def __init__(self):
        '''Loads configuration file, initialise databases to store output data and queue of channel's id's.
        '''

        config = self.load_config_file()
        self.fifo = fifo_youtube.Fifo_queue(config)  # queue of nodes left to check
        self.manage_data = databases_youtube.data(config)

        self.app_key = config['key']
        self.url = config['url']

    pass

    def load_config_file(self):
        try:
            stream = open('config/config_youtube.yml', 'r')
        except:
            try:
                stream = open('config/config_youtube.yaml', 'r')
            except IOError:
                print IOError

        return yaml.load(stream)
        pass

    def get_url(self, id=None):
        '''Creates url adress to make request to google.api/youtube.

                 Args:
                    id (int, optionally): Default is None. Specific number of currently retrieving channel

                 Returns:
                    url (string): url adress neccessery to make api request on.
               '''
        url = self.url % (id, self.app_key)
        url = str(url)
        print url
        return url


    pass


    def get_json(self, url):
        '''Makes request to gooogle api, loads returned json to device memory.

       Args:
          url (string): adress to make request on

    '''
        data = urllib.urlopen(url)
        self.json = json.load(data)

        if self.json.get('error'):
            print "url failed to get"
    pass


    def retrieve(self):
        '''In youtube case retrieving is based on featured channels url id's.
       For each channel included in data loaded from json file there are doing the following steps:
       check if channel is not already in database,
       (if no)
       check wether channel contains not empty 'featuredChannelsUrls' parameter,
       (if yes) add all parameter into queue, insert channel into output database
        (if no) insert channel into database

        '''

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


def retrieve():
    '''Basic function which do steps to retrieve all of wanted data from youtube.
       It is looping until fifo queue is empty.

       There are to options to start process:
       1. Start from 'z dupy' channel node (id of that channel)
       2. Start from previously stopped node (channel id stored in queue)

    '''


b = BFS_retrieving()  # b.manage_data.check_pages()
next_id = 0
if b.fifo.is_empty():
    while b.fifo.is_empty():
        last_id = next_id
        next_id = b.fifo.get()[0]
        if next_id is not last_id:
            b.fifo.remove(next_id)

            b.get_json(b.get_url(next_id))
            b.retrieve()
            print "current id", next_id
else:
    id = 'UCNhtAn1MT600xiNiX8ErnyQ'
    b.get_json(b.get_url(id))
    b.retrieve()

    while b.fifo.is_empty():  ##exploring graph
        last_id = next_id
        next_id = b.fifo.get()[0]
        if next_id is not last_id:
            b.fifo.remove(next_id)

            b.get_json(b.get_url(next_id))
            b.retrieve()
            print "current id", next_id
    pass

b.fifo.close()
pass

if __name__ == '__main__':
    retrieve()
