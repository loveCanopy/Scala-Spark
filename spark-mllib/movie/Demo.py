from pyspark import SparkContext,SparkConf
conf=SparkConf().setMaster("local").setAppName("hello")
sc=SparkContext(conf=conf)
user_data=sc.textFile(r'E:\mllib\ml-100k\ml-100k\u.user')
import matplotlib.pyplot as plt
import pylab as pl
user_fields=user_data.map(lambda line: line.split("|"))
num_users=user_fields.map(lambda fields:fields[0]).count()

num_genders=user_fields.map(lambda fields:fields[2]).distinct().count()

num_occupations=user_fields.map(lambda fields:fields[3]).distinct().count()

num_zipcodes=user_fields.map(lambda fields:fields[4]).distinct().count()

# print("user:%d,genders:%d,occupations:%d,zip code:%d" %(num_users,num_genders,num_occupations,num_zipcodes))

# ages=user_fields.map(lambda x:int(x[1])).collect()
# pl.hist(ages,bins=20,color="lightblue",normed=True)#ages数组，直方图bins数目，true正则化直方图
# fig=plt.gcf()#获取当前的绘图对象
# fig.set_size_inches(16,10)
# plt.show()

count_by_occuption=user_fields.map(lambda fields:(fields[3],1)).reduceByKey(lambda x ,y:x+y).collect()
import numpy as np

x_axis1=np.array([c[0] for c in count_by_occuption])
y_axis1=np.array([c[1] for c in count_by_occuption])

x=x_axis1[np.argsort(y_axis1)]
y=y_axis1[np.argsort(y_axis1)]

pos=np.arange(len(x))
width=1.0
ax=plt.axes()
ax.set_xticks(pos+(width/2))
ax.set_xticklabels(x)

plt.bar(pos,y,width,color="lightblue")
plt.xticks(rotation=30)
fig1=plt.gcf()
fig1.set_size_inches(16,10)
plt.show()