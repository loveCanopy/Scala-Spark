#文本特征  提取数据集中的电影标题，为二元矩阵
from pyspark import SparkContext,SparkConf
conf=SparkConf().setMaster("local").setAppName("item")
sc=SparkContext(conf=conf)
movie_data=sc.textFile(r'E:\mllib\ml-100k\ml-100k\u.item')
movie_fields=movie_data.map(lambda x:x.split("|"))
movie_name=movie_fields.map(lambda fields:fields[1])
#设计函数处理标题
def extract_title(raw):
    import re
    grps=re.search("\((\w+)\)",raw)
    if grps:
        # print(grps.start())
        return raw[:grps.start()].strip()
    else:
        return raw
movie_titles=movie_name.map(lambda m:extract_title(m))
title_terms=movie_titles.map(lambda x:x.split(" "))
all_term=title_terms.flatMap(lambda x:x).distinct().collect()
#构建词到序号的字典映射
idx=0
all_items_dict={}
for term in all_term:
    all_items_dict[term]=idx
    idx+=1
#词集合转化为scipy稀疏向量
def create_vector(terms,terms_dict):
    from scipy import sparse as sp
    num_terms=len(terms_dict)
    x=sp.csc_matrix((1,num_terms))
    for t in terms:
        if t in terms_dict:
            idx=terms_dict[t]
            x[0,idx]=1
    return x
#广播变量，共享内存
all_term_bcast=sc.broadcast(all_items_dict)
terms_vector=title_terms.map(lambda term:create_vector(term,all_term_bcast.value))
print(terms_vector.take(5))