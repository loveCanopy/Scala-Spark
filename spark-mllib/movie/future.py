from pyspark import SparkContext,SparkConf
conf=SparkConf().setMaster("local").setAppName("hello")
sc=SparkContext(conf=conf)
user_data=sc.textFile(r'E:\mllib\ml-100k\ml-100k\u.user')
import matplotlib.pyplot as plt
import pylab as pl
user_fields=user_data.map(lambda line: line.split("|"))
all_occupations=user_fields.map(lambda fields:fields[3]).distinct().collect()
all_occupations.sort()
#类别特征用向量表示
idx=0
all_occupations_dict={}
for o in all_occupations:
    all_occupations_dict[o]=idx
    idx+=1

import numpy as np
k=len(all_occupations_dict)
binary_x=np.zeros(k)
k_engineer=all_occupations_dict['engineer']
binary_x[k_engineer]=1
print(binary_x)