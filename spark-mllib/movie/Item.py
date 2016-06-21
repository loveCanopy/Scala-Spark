from pyspark import SparkContext,SparkConf
conf=SparkConf().setMaster("local").setAppName("item")
sc=SparkContext(conf=conf)
movie_data=sc.textFile(r'E:\mllib\ml-100k\ml-100k\u.item')
print(movie_data.first())
num_movie=movie_data.count()
print("movies:%d"%num_movie)

def convert_year(x):
    try:
        return int(x[-4:])
    except:
        return 1900
import matplotlib.pyplot as plt
from matplotlib.pyplot import hist
movie_fields = movie_data.map(lambda lines:lines.split('|'))
years = movie_fields.map(lambda fields: fields[2]).map(lambda x: convert_year(x))
years_filtered = years.filter(lambda x: x!=1900)
print (years_filtered.count())
movie_ages = years_filtered.map(lambda yr:1998-yr).countByValue()
values = movie_ages.values()
bins = movie_ages.keys()
hist(list(values),bins=list(bins),color='lightblue',normed=True)
fig = plt.gcf()
fig.set_size_inches(8,5)
plt.show()