from pyspark import SparkContext,SparkConf
conf=SparkConf().setMaster("local").setAppName("rating")
sc=SparkContext(conf=conf)
movie_data=sc.textFile(r'E:\mllib\ml-100k\ml-100k\u.data')

rating_data=movie_data.map(lambda x : x.split("\t"))
rating=rating_data.map(lambda fields:int(fields[2]))
min_rating=rating.reduce(lambda x,y:min(x,y)) #最低评分
max_rating=rating.reduce(lambda x,y:max(x,y)) #最高评分
num_rating=rating_data.count() #评级数量

mean_rating=rating.reduce(lambda x,y:x+y)/num_rating #平均评分
import numpy as np
median_rating=np.median(rating.collect()) #评分中位数
user_data=sc.textFile(r'E:\mllib\ml-100k\ml-100k\u.user')
user_fields=user_data.map(lambda line: line.split("|"))
num_users=user_fields.map(lambda fields:fields[0]).count() #用户数量
ratings_per_users=num_rating/num_users  #每个用户的评分量
movie_data=sc.textFile(r'E:\mllib\ml-100k\ml-100k\u.item')
num_movie=movie_data.count()
rating_per_movies=num_rating/num_movie #每个电影的评分量

# print(min_rating,max_rating,mean_rating,ratings_per_users,rating_per_movies)

count_by_rating=rating.countByValue()
x_axis=np.array(list(count_by_rating.keys()))
y_axis=np.array([float(c) for c in count_by_rating.values()])

y_axis_normed=y_axis/y_axis.sum()
import matplotlib.pyplot as plt
pos=np.arange(len(x_axis))
width=1
ax=plt.axes()
ax.set_xticks(pos+(width/2))
ax.set_xticklabels(x_axis)
plt.bar(pos,y_axis_normed,width,color="lightblue")
plt.xticks(rotation=30)
fig=plt.gcf()
fig.set_size_inches(16,10)
plt.show()



#每个用户的评级次数
user_rating_group=rating_data.map(lambda fields:(fields[0],fields[1])).groupByKey()
user_rating_byuser=user_rating_group.map(lambda k_v:(k_v[0],len(k_v[1])))

user_rating_byuser_local=user_rating_byuser.map(lambda k_v:k_v[1]).collect()
from matplotlib.pyplot import hist
hist(user_rating_byuser_local,bins=200,color="lightblue",normed=True)
fig=plt.gcf()
fig.set_size_inches(16,10)
plt.show()


