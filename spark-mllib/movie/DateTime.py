#原始的时间数据到评级发生的点钟的类别

from pyspark import SparkContext,SparkConf
conf=SparkConf().setMaster("local").setAppName("rating")
sc=SparkContext(conf=conf)
movie_data=sc.textFile(r'E:\mllib\ml-100k\ml-100k\u.data')

rating_data=movie_data.map(lambda x : x.split("\t"))

def extarct_datatime(ts):
    import datetime
    return datetime.datetime.fromtimestamp(ts)

timestamps=rating_data.map(lambda fields:int(fields[3]))
hour_of_day=timestamps.map(lambda ts:extarct_datatime(ts).hour)

def assign_tod(hr):
    times_of_day={
        'moring':range(7,12),
        'lunch':range(12,14),
        'afternon':range(14,18),
        "evening":range(18,23),
        "night":[23,24,1,2,3,4,5,6]
    }
    for k,v in times_of_day.items():
        if hr in v:
            return k
time_of_day=hour_of_day.map(lambda hr:assign_tod(hr))
print(time_of_day.take(5))