import urllib,json,yaml
import fifo_youtube,databases_youtube
import guess_language

class BFS_retrieving:

    def __init__(self):
        '''Initialise databases including a queue and loading a config file'''

        config = self.load_config_file()
        self.fifo = fifo_youtube.Fifo_queue(config)  #queue of nodes left to check
        self.manage_data = databases_youtube.data(config)


        self.app_key = config['key']
        self.url = config['url']
    pass

    def load_config_file(self):
        try:
            stream = open('config/config_youtube.yml', 'r')
        except:
            try:
                stream = open('config/config_youtube.yaml','r')
            except IOError:
                print IOError

        return yaml.load(stream)
    pass

    def get_url(self, id =None):
        url =self.url%(id,self.app_key)
        url = str(url)
        print url
        return url
    pass

    def get_json(self, url):
        data = urllib.urlopen(url)
        self.json = json.load(data)

        if self.json.get('error'):
            print "url failed to get"
    pass

    def retrieve(self):
        '''In youtube case retrieving is based on featuredChannelsUrls'''

        items = self.json['items']

        for item in items:
            if 'featuredChannelsUrls' in item['brandingSettings']['channel']:
                featuredChannelsUrls= item['brandingSettings']['channel']['featuredChannelsUrls']
            else:
                print 'empty parameter featuredChannelsUrls'
                self.manage_data.insert_into(item)
                break

            check = self.manage_data.check_item(item['id'])
            if check is not None:
                print "I've been here"
                break
            else:
                for url in featuredChannelsUrls:
                    self.fifo.put(url)

                self.manage_data.insert_into(item)
    pass

#guess in this case is worthless becouse it is available parameter in json which concrete language of each channel
    def language_check(self, name, about=None, description=None):
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



def retrieve():
    b = BFS_retrieving()
    # b.manage_data.check_pages()
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