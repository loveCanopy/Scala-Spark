import numpy as np
np.random.seed(42)
#一个样本，满足正太分布 个数为10
x=np.random.randn(10)

#向量的二阶范数 x中各个元素平方之和再开根号
norm_x_2=np.linalg.norm(x)
# X/二阶范数
normalized_x=x/norm_x_2
normalized_x_2=np.linalg.norm(normalized_x)

# from pyspark.mllib.feature import Normalizer
# normalizer=Normalizer()
# vector=sc.parallelize([x])
# normalized_x_mllib=normalizer.tranform(vector).first().toArray() ==normalized_x
