from pyspark import SparkContext
from pyspark import SparkConf


sparkconf = SparkConf().setMaster("local[*]")
sparkcontext = SparkContext.getOrCreate(sparkconf)
albums = sparkcontext.textFile("../datasets/albums.csv")
albumMapping = albums.map(lambda x: x.split(","))

# Find the average critic, mapping the results
avgCritic = albumMapping.map(lambda album: (album[0], (float(album[7]) + float(album[8]) + float(album[9]))/3))

# Get top 10 without making it a list and back to rdd.
# Could also have used .take(10) but then I would have to parallelize again.

result = avgCritic\
                .sortBy(lambda x: x[1], False)\
                .zipWithIndex()\
                .filter(lambda x: x[1] < 10)


result.map(lambda res: str(res[0][0]) + "\t" + str(res[0][1])).coalesce(1).saveAsTextFile('result_6.tsv')






