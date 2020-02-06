from pyspark.sql import SparkSession
import psycopg2
from textblob import TextBlob

def readfile(filepath):
    try:
        file = open(filepath)
    except IOError:
        print(IOError)
        return
    result = file.read().splitlines()
    file.close()
    return result

def store_result(result):

    config = readfile("/home/hadoop/config.txt")
    db = "dbname=" +  config[0] + " " \
         + "user=" + config[1] + " " \
         + "host=" + config[2] + " " \
         + "password=" + config[3]
    try:
        conn = psycopg2.connect(db)
    except:
        print("I am unable to connect to the database")
        return

    conn.autocommit = True

    cur = conn.cursor()

    cur.execute("SELECT * FROM sentiments where word = '{}'".format(target))
    entry = cur.fetchall()

    if not entry:
         cur.execute("""INSERT INTO sentiments (word, count, positive, negative, neutral)
                       VALUES ('{}', {}, {}, {}, {})""".format(result[0], result[1], result[2], result[3], result[4]))
    

if __name__=="__main__":

    spark = SparkSession\
        .builder\
        .appName("CommonCrawlPro")\
        .getOrCreate()

    target = "nike"

    bucket = "s3://commoncrawl/"
    segments = readfile("/home/hadoop/wet.paths")
    paths = []
    for segment in segments:
        paths.append(bucket+segment)

    path = ",".join(paths)

    webpages = spark.sparkContext.textFile(path)
    filtered_webpages = webpages.filter(lambda line: target in line)


    def sentiment_analysis(line):
        return TextBlob(line).sentiment.polarity

    sentiments = filtered_webpages.map(lambda line: sentiment_analysis(line))

    def fun(x):
        if x <  0: return -1
        elif x == 0: return 0
        else: return 1

    sentiments_count = sentiments.map(lambda score: fun(score)) \
                                 .map(lambda label: (label, 1)) \
                                 .reduceByKey(lambda a, b: a + b) \
                                 .collect()
    pos = neg = neu = 0
    for item in sentiments_count:
        if item[0] == 0:
            neu = item[1]
        elif item[0] == -1:
            neg = item[1]
        else:
            pos = item[1]

    print("########################################")
    print("Positive = {}, Negative = {}, Neutral = {}".format(pos, neg, neu))
    print("########################################")

    result = [target, pos+neg+neu, pos, neg, neu]
 
    store_result(result)

    spark.stop()


