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

# Get every artist from Norway (use lowercase comparison)
artistInfo = artistMapping\
                        .map(lambda artist: (artist[0], (artist[5])))\
                        .filter(lambda artist: artist[1].lower() == 'norway')

# Artist_id and mtv_critic from albums
albumInfo = albumMapping.map(lambda album: (album[1], album[8]))

# Join albumInfo with the artists from Norway
joinedInfo = artistInfo.join(albumInfo)

# Calculate the average critic from mtv for each artist from Norway
# First making a tupple to easier aggregate per artist_id
# Then aggregating and taking the average over the sum.
aggregationTuple = joinedInfo.map(lambda tup: (tup[0], tup[1][1]))

averagePerID = aggregationTuple.aggregateByKey((0.0, 0),
                                        lambda iTotal, oisubtotal:  (float(iTotal[0]) + float(oisubtotal), float(iTotal[1]) + 1),
                                        lambda fTotal, iTotal: (float(fTotal[0]) + float(iTotal[0]), float(fTotal[1]) + float(iTotal[1])))\
                    .mapValues(lambda val: val[0]/val[1])


result = artistMapping\
                    .map(lambda x: (x[0], (x[1], x[2], x[5])))\
                    .join(averagePerID)\
                    .sortBy(lambda res: (str(res[1][0][0]) if str(res[1][0][1]) == '' else str(res[1][0][1])), True)\
                    .sortBy(lambda x: float(x[1][1]), False)\


result.map(lambda res: (str(res[1][0][0]) if str(res[1][0][1]) == '' else str(res[1][0][1])) + "\t" + str(res[1][0][2]) + "\t" + str(res[1][1])).coalesce(1).saveAsTextFile('result_9.tsv')
