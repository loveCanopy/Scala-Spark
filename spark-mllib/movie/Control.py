from pyspark import SparkContext,SparkConf
conf=SparkConf().setMaster("local").setAppName("control")
sc=SparkContext(conf=conf)
movie_data=sc.textFile(r'E:\mllib\ml-100k\ml-100k\u.item')
def convert_year(x):
    try:
        return int(x[-4:])
    except:
        return 1900
movie_fields = movie_data.map(lambda lines:lines.split('|'))
years = movie_fields.map(lambda fields: fields[2]).map(lambda x: convert_year(x)).collect()
import numpy as np
years_pre_processed_array=np.array(years)
mean_year=np.mean(years_pre_processed_array[years_pre_processed_array!=1900])
median_year=np.median(years_pre_processed_array[years_pre_processed_array!=1900])
index_bad_data=np.where(years_pre_processed_array==1900)[0][0]
years_pre_processed_array[index_bad_data]=median_year
print(mean_year,median_year,np.where(years_pre_processed_array==1900)[0])