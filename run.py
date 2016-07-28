'''
Crawler application for social media.
Executable part of whole project. Here are invoked scripts responsible for 3 of web portals.

Example:
    $ python run.py facebook
'''


import sys,subprocess
from multiprocessing import Process, Pool
import time

def f(argument):
    '''Executing subprocess depend on given parameter.

    Args:
       argument (str) - should be one of the 3 options: facebook, twitter, youtube.
           otherwise it causes nothink
   
    '''
    if argument == "facebook":
        process1 = subprocess.call(['python', 'graph_retrieving_facebook.py'])
    elif argument == 'youtube':
        process2 = subprocess.call(['python','graph_retrieving_youtube.py'])
        pass
    elif argument == 'twitter':
        process3 = subprocess.call(['python','graph_retrieving_twitter.py'])
        pass
    else:
        print 'bad argument given'


if __name__ == '__main__':
    '''Opens workers which number is based on amount of parameters given as a parameter.
       For each worker invoking method f()
    '''
    p = Pool(len(sys.argv)-1)
    for count in range(1, len(sys.argv)):
        p = Process(target=f, args=(sys.argv[count],))
        p.start()
        print "Executed " + str(sys.argv[count]) + "crawler"
        time.sleep(10)
