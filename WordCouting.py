

import collections
from pyspark  import SparkConf, SparkContext


conf = SparkConf().setMaster('local').setAppName('Word counting')
sc = SparkContext.getOrCreate(conf = conf)

myRDD = (sc.textFile('/content/sample_data/abc.txt', minPartitions = 1, use_unicode = True).map(lambda each: each.split(' ')))
counts = myRDD.reduce(lambda a:1)
words = len(counts)
print(words)

myrdd = sc.parallelize(counts)
temp = myrdd.map(lambda a:(a,1))
number_of_word = temp.reduceByKey(lambda a,b: a+b)
#print(number_of_word.collect())
k=int(input('Enter k: '))
result = number_of_word.takeOrdered(k, key = lambda a: -a[1])
for i in result:
  print(i[0])
#max_i=number_of_word.reduce(lambda a,b: max(a,b))
#print(max_i)