import urllib, json, yaml
import fifo_facebook, databases_facebook
import guess_language


class BFS_retrieving:
    '''Class implements BFS graph algorithm and its helpful method in order to collect data from facebook.

       Executed correctly class methods effectively gives huge amount of data stored in specified database.
    '''

    def __init__(self):
        '''Loads configuration file, initialise databases to store output data and queue of page's id's.

        '''

        config = self.load_config_file()

        self.fifo = fifo_facebook.Fifo_queue(config)  # queue of nodes left to check
        self.manage_data = databases_facebook.data(config)

        self.set_access_token(config['access_token'])
        self.url = config['url']
    pass


    def load_config_file(self):

        try:
            stream = open('config/config_facebook.yml', 'r')
        except:
            try:
                stream = open('config/config_facebook.yaml', 'r')
            except IOError:
                print IOError

        return yaml.load(stream)

    pass


    def get_url(self, id=None):
        '''Creates url adress to make request to facebook graph api.

       Args:
          id (int, optionally): Default is None. Specific number of currently retrieving page

       Returns:
          url (string): url adress neccessery to make api request on.
    '''
        if id is None:
            url = self.url % ('sotrender', self.access_token)
            url = str(url)
            print url
            return url
        else:
            url = self.url % (str(id), self.access_token)
            url = str(url)
            print url
            return url

        pass


    def set_access_token(self, access_token):
        self.access_token = access_token

    pass


    def get_json(self, url):
        '''Makes request to facebook graph api, loads returned json to device memory.

       Args:
          url (string): adress to make request on

    '''
        data = urllib.urlopen(url)
        self.json = json.load(data)

        if self.json.get('error'):
            print 'Probably your access token is invalid, trying one more time'
            self.get_json(url)

    pass


    def retrieve(self):
        '''In facebook case retrieving is based on id's of particular page.
       For each page included in data loaded from json file there are doing the following steps:
       check if that page is already in database,
       (if not) put its id into queue,
       check language of such page,
       insert into output database

    '''

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


def run():


    '''Basic function which do steps to retrieve all of wanted data from facebook.
       It is looping until fifo queue is empty.

       There are to options to start process:
       1. Start from sotrender node (id of sotrender)
       2. Start from previously stopped node (page id stored in queue)

    '''

b = BFS_retrieving()
next_id = 0
if b.fifo.is_empty():
    while b.fifo.is_empty():  ##exploring graph
        last_id = next_id
        next_id = b.fifo.get()[0]
        if next_id is not last_id:
            b.fifo.remove(next_id)
            b.get_json(b.get_url(id=next_id))
            b.retrieve()
            print "current id", next_id
else:
    b.get_json(b.get_url())
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
    run()
