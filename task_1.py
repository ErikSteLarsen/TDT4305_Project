from pyspark import SparkContext
from pyspark import SparkConf


conf = SparkConf().setMaster("local[*]")
context = SparkContext.getOrCreate(conf)
albums = context.textFile("../datasets/albums.csv")
albumsSplit = albums.map(lambda x: x.split(","))

distinctGenres = albumsSplit.map(lambda album: album[3].lower()).distinct().count()

print("Number of distinct genres: " + str(distinctGenres))


