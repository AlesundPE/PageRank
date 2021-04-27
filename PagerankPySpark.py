def destReceive(data):
    dests, PageRank = data[1]
    contribution = []
    for dest in dests:
        contribution.append((dest,PageRank/len(dests)))
    return contribution
import findspark
findspark.init("D:/spark-3.0.1-bin-hadoop2.7") # you need to specify the path to PySpark here.
from pyspark import SparkContext
import time
iteration = 5
total_time = []
sc = SparkContext(appName = "PageRank")
lines = sc.textFile("C:/Users/90381/Desktop/links") # you need to change the path to graph here
links = lines.flatMap(lambda x : x.split("\n")).map(lambda x : x.split()).map(lambda x : (x[0],x[1].split(","))).persist()
ranks = links.mapValues(lambda x : 1)
for i in range(iteration):
    start = time.time()
    ranks = links.join(ranks).partitionBy(2).flatMap(lambda x : destReceive(x)).reduceByKey(lambda x, y : x + y).mapValues(lambda x : 0.15 + 0.85*x) # you can change the number of partition here by modifing the parameter of partitionBy()
    ranks.collect()
    print(ranks.getNumPartitions())
    end = time.time()
    print("iteration {} takes {} seconds".format(i, end - start))
    total_time.append(end-start)
    
print("Average is {}".format(sum(total_time)/len(total_time)) )
sc.stop()