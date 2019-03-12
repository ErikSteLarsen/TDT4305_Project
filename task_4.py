from pyspark import SparkContext
from pyspark import SparkConf


sparkconf = SparkConf().setMaster("local[*]")
sparkcontext = SparkContext.getOrCreate(sparkconf)
albums = sparkcontext.textFile("../datasets/albums.csv")
albumMapping = albums.map(lambda x: x.split(","))

albumInfo = albumMapping.map(lambda album: (album[1], album[0]))

result = albumInfo\
                .map(lambda w: (w[0], 1))\
                .reduceByKey(lambda x, y: x + y)\
                .sortBy(lambda y: int(y[0]), True)\
                .sortBy(lambda x: x[1], False)


result.map(lambda res: str(res[0]) + "\t" + str(res[1])).coalesce(1).saveAsTextFile('result_4.tsv')
