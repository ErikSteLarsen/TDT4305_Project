from pyspark import SparkContext
from pyspark import SparkConf


sparkconf = SparkConf().setMaster("local[*]")
sparkcontext = SparkContext.getOrCreate(sparkconf)
artists = sparkcontext.textFile("../datasets/artists.csv")
artistMapping = artists.map(lambda x: x.split(","))

artistInfo = artistMapping.map(lambda artist: (artist[5], artist[0]))

result = artistInfo\
                .map(lambda w: (w[0], 1))\
                .reduceByKey(lambda x, y: x + y)\
                .sortBy(lambda y: y[0], True)\
                .sortBy(lambda x: x[1], False)


result.map(lambda res: res[0].encode("utf-8") + "\t" + str(res[1])).coalesce(1).saveAsTextFile('result_3.tsv')
