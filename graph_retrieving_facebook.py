import urllib,json,yaml
import fifo_facebook,databases_facebook
import guess_language

class BFS_retrieving:

    def __init__(self):
        '''Initialise databases including a queue and loading a config file'''

        config = self.load_config_file()

        self.fifo = fifo_facebook.Fifo_queue(config)  #queue of nodes left to check
        self.manage_data = databases_facebook.data(config)

        self.set_access_token(config['access_token'])
        self.url = config['url']

    pass

    def load_config_file(self):
        try:
            stream = open('config/config_facebook.yml', 'r')
        except:
            try:
                stream = open('config/config_facebook.yaml','r')
            except IOError:
                print IOError

        return yaml.load(stream)
    pass

    def get_url(self, id =None):
        if id is None:
            url=self.url%('sotrender',self.access_token)
            url = str(url)
            print url
            return url
        else:
            url = self.url%(str(id), self.access_token)
            url = str(url)
            print url
            return url
    pass

    def set_access_token(self, access_token):
        self.access_token = access_token
    pass

    def get_json(self, url):
        data = urllib.urlopen(url)
        self.json = json.load(data)

        if self.json.get('error'):
            print 'Probably your access token is invalid'
    pass

    def retrieve(self):
        '''In facebook case retrieving is based on id's of particular page.
            In order to retrieve all pages from portal it is used BFS algorithm'''

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

                language = self.language_check(name,about,description)

                self.manage_data.insert_into(page, language)
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

        lang = guess_language.guessLanguage(text)
        return lang



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




