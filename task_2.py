from pyspark import SparkContext
from pyspark import SparkConf


conf = SparkConf().setMaster("local[*]")
context = SparkContext.getOrCreate(conf)
artists = context.textFile("../datasets/artists.csv")
artistMapping = artists.map(lambda x: x.split(","))

oldestArtist = artistMapping.map(lambda artist: artist[4]).min()

print("Year of birth of oldest artist:" + str(oldestArtist))

