from pyspark import SparkContext
from pyspark import SparkConf


sparkconf = SparkConf().setMaster("local[*]")
sparkcontext = SparkContext.getOrCreate(sparkconf)
albums = sparkcontext.textFile("../datasets/albums.csv")
albumMapping = albums.map(lambda x: x.split(","))

albumInfo = albumMapping.map(lambda album: (album[3], album[6]))

result = albumInfo\
                .map(lambda w: (w[0], int(w[1])))\
                .reduceByKey(lambda x, y: x + y)\
                .sortBy(lambda y: y[0], True)\
                .sortBy(lambda x: x[1], False)


result.map(lambda res: str(res[0]) + "\t" + str(res[1])).coalesce(1).saveAsTextFile('result_5.tsv')
