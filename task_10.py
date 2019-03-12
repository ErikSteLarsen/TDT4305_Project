from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import countDistinct

sparkConf = SparkConf().setMaster("local[*]")
sparkContext = SparkContext.getOrCreate(sparkConf)
sqlContext = SQLContext(sparkContext)

albums = sparkContext.textFile('../datasets/albums.csv')
albumMapping = albums.map(lambda x: x.split(","))
albumRDD = albumMapping.map(lambda x: (int(x[0]),int(x[1]),x[2],x[3],x[4],int(x[5]),int(x[6]),float(x[7]),float(x[8]),float(x[9])))

artists = sparkContext.textFile('../datasets/artists.csv')
artistMapping = artists.map(lambda x: x.split(","))
artistRDD = artistMapping.map(lambda x: (int(x[0]),x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8]))

albumSchema = StructType([
        StructField("id", IntegerType(), True),
        StructField("artist_id", IntegerType(), True),
        StructField("album_title", StringType(), True),
        StructField("genre", StringType(), True),
        StructField("year_of_pub", StringType(), True),
        StructField("num_of_tracks", IntegerType(), True),
        StructField("num_of_sales", IntegerType(), True),
        StructField("rolling_stone_critic", FloatType(), True),
        StructField("mtv_critic", FloatType(), True),
        StructField("music_maniac_critic", FloatType(), True),
])

artistSchema = StructType([
        StructField("id", IntegerType(), True),
        StructField("real_name", StringType(), True),
        StructField("art_name", StringType(), True),
        StructField("role", StringType(), True),
        StructField("year_of_birth", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("email", StringType(), True),
        StructField("zip_code", StringType(), True),
    ])

albumDF = sqlContext.createDataFrame(albumRDD, albumSchema)
artistDF = sqlContext.createDataFrame(artistRDD, artistSchema)


# Task A) and D) solved here, with aggregation
artistValues = artistDF.agg(countDistinct("id"), countDistinct("country"))
artistValues.show()

# Task B) and C), also solved with aggregation over distinct elements in the two columns
albumValues = albumDF.agg(countDistinct("id"), countDistinct("genre"))
albumValues.show()

# Task E) min year_of_pub
minPublish = albumDF.agg({"year_of_pub": "min"})
minPublish.show()

# Task F) max year_of_pub
maxPublish = albumDF.agg({"year_of_pub": "max"})
maxPublish.show()

# Task G) min year_of_birth
minBirth = artistDF.agg({"year_of_birth": "min"})
minBirth.show()

# Task H max year_of_birth
maxBirth = artistDF.agg({"year_of_birth": "max"})
maxBirth.show()



