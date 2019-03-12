from pyspark import SparkContext
from pyspark import SparkConf


sparkconf = SparkConf().setMaster("local[*]")
sparkcontext = SparkContext.getOrCreate(sparkconf)

# Load in albums
albums = sparkcontext.textFile("../datasets/albums.csv")
albumMapping = albums.map(lambda x: x.split(","))

# Load in artists
artists = sparkcontext.textFile("../datasets/artists.csv")
artistMapping = artists.map(lambda x: x.split(","))
artistAndCountry = artistMapping.map(lambda artist: (artist[0], artist[5]))


# As in task_6, finding the top 10 rated albums, and also include the artist ID
avgCritic = albumMapping.map(lambda album: (album[1], (album[0], (float(album[7]) + float(album[8]) + float(album[9]))/3)))
# I did this with .take(10) because my results were already complicated, so I didnt want to mess it up.
top10 = sparkcontext.parallelize(avgCritic.sortBy(lambda x: x[1][1], False).take(10))


# Join artists and album values on artist_id
joinedInfo = artistAndCountry.join(top10).sortBy(lambda x: x[1][1][0], True)

joinedInfo.map(lambda res: str(res[1][1][0]) + "\t" + str(res[1][1][1]) + "\t" + res[1][0]).coalesce(1).saveAsTextFile('results/result_7.tsv')
