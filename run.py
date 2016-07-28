import sys,subprocess
from multiprocessing import Process, Pool
import time

def f(argument):
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
    p = Pool(len(sys.argv)-1)
    for count in range(1, len(sys.argv)):
        p = Process(target=f, args=(sys.argv[count],))
        p.start()
        print "Executed " + str(sys.argv[count]) + "crawler"
        time.sleep(10)
