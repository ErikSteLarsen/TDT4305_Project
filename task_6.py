from pyspark import SparkContext
from pyspark import SparkConf


sparkconf = SparkConf().setMaster("local[*]")
sparkcontext = SparkContext.getOrCreate(sparkconf)
albums = sparkcontext.textFile("../datasets/albums.csv")
albumMapping = albums.map(lambda x: x.split(","))

# Find the average critic, mapping the results
avgCritic = albumMapping.map(lambda album: (album[0], (float(album[7]) + float(album[8]) + float(album[9]))/3))

result = avgCritic\
                .sortBy(lambda x: x[1], False)\
                .take(10)

resultEnd = sparkcontext.parallelize(result)

resultEnd.map(lambda res: str(res[0]) + "\t" + str(res[1])).coalesce(1).saveAsTextFile('results/result_6.tsv')






