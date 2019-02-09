import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt



def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")
    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)
    

def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    poscnt=list()
    negcnt=list()
    for x in counts:
        if(x[0][0]=="positive"):
            poscnt.append(x[0][1])
            negcnt.append(x[1][1])
        elif(x[0][0]=="negative"):
            poscnt.append(x[1][1])
            negcnt.append(x[0][1])
    
    fig = plt.figure()

    #all plot code here.

    plt.plot(poscnt,'-r', label = 'positive')
    plt.plot(negcnt,'-b', label = 'negative')
    plt.ylabel('Word count')
    plt.xlabel('Time step')
    plt.legend(loc='upper left')
    #plt.axis([-1, 12, 0, 300])
    
    fig.savefig("xyz.png")
    #plt.show()
    


def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    wordslist=open(filename).read().strip().split('\n')
    #print(wordslist)
    return wordslist

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount) 


def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    
    # Split each tweets into words
    words = tweets.flatMap(lambda line: line.split(" "))
    
    classes = words.map(lambda word: ("positive", 1) if word in pwords else (("negative", 1) if word in nwords else (word, 1)))
    
    #filtering out words that do not fall in either positive or negative
    classes = classes.filter(lambda x: x[0] == 'positive' or x[0] == 'negative')
    tc = classes.reduceByKey(lambda x, y: x + y)
    #tc.pprint()
    runningc = tc.updateStateByKey(updateFunction)
    runningc.pprint()
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    tc.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts




if __name__=="__main__":
    main()
