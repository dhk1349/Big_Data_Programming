import sys
import re
from operator import add

from pyspark import SparkContext

def map_phase(x):
    x = re.sub('--', ' ', x)
    x = re.sub("'", '', x)
    return re.sub('[?!@#$\'",.;:()]', '', x).lower()

if __name__ == "__main__":
	if len(sys.argv) < 4:
        print >> sys.stderr, "Usage: wordcount <master> <inputfile> <outputfile>"
        exit(-1)
	sc = SparkContext(sys.argv[1], "python_wordcount_sorted in bigdataprogrammiing")
	lines = sc.textFile(sys.argv[2],2)
	print(lines.getNumPartitions()) # print the number of partitions
    words=lines.map(lambda x: x.lower())
    words=words.filter(lambda x: x!='tokyo')
    words=words.filter(lambda x: x!='neighborhood')
    words=words.map(lambda x: (x, 1))
    result=words.reduceByKey(lambda x,y :x+y)
    result=result.sortBy(lambda x: x[0])
	result.saveAsTextFile(sys.argv[3])
