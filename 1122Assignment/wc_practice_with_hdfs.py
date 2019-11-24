import sys
import re
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession

def map_phase(x):
    x = re.sub('--', ' ', x)
    x = re.sub("'", '', x)
    return re.sub('[?!@#$\'",.;:()]', '', x).lower()

if __name__ == "__main__":
	if len(sys.argv) < 4:
		print >> sys.stderr, "Usage: wordcount <master> <inputfile> <outputfile>"
		exit(-1)
    
    	sparkSession = SparkSession.builder.appName("test").getOrCreate()
    	#data=open(sys.argv[2])
    	df=sparkSession.createDataFrame(sys.argv[2])
    	df.write.csv("hdfs://cluster/user/hdfs/test/example.csv")
    	df_load = sparkSession.read.csv('hdfs://cluster/user/hdfs/test/example.csv')
	#sc = SparkContext(sys.argv[1], "python_wordcount_sorted in bigdataprogrammiing")
	lines = sc.textFile(df_load,2)
	print(lines.getNumPartitions()) # print the number of partitions
   	words=lines.map(lambda x: x.lower())
   	words=words.filter(lambda x: x!='tokyo')
    	words=words.filter(lambda x: x!='neighborhood')
    	words=words.map(lambda x: (x, 1))
    	result=words.reduceByKey(lambda x,y :x+y)
    	result=result.sortBy(lambda x: x[0])
	result.saveAsTextFile(sys.argv[3])
