from pyspark import SparkContext
from pyspark import SparkConf

sparkconf = SparkConf().setMaster("local[*]")
sparkcontext = SparkContext.getOrCreate(sparkconf)

# Load in albums
albums = sparkcontext.textFile("../datasets/albums.csv")
albumMapping = albums.map(lambda x: x.split(","))

# Load in artists, with id and art_name
artists = sparkcontext.textFile("../datasets/artists.csv")
artistMapping = artists.map(lambda x: x.split(","))
artistInfo = artistMapping.map(lambda artist: (artist[0], (artist[1], artist[2])))

# Get artist_id for albums with "mtv_critic == 5"
albumInfo = albumMapping.map(lambda album: (album[1], album[8]))
albumsFiltered = albumInfo.filter(lambda album: float(album[1]) == 5.0)

result = artistInfo.join(albumsFiltered).map(lambda res: res[1][0][0] if res[1][0][1] == '' else res[1][0][1])
distinctResult = result.distinct().sortBy(lambda x: x[0])

distinctResult.coalesce(1).saveAsTextFile('results/result_8.tsv')
