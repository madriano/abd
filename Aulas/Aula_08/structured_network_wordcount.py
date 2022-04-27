# Apache Spark Structured Streaming example from documentation

# How to run this simple example

# First, you need to run Netcat (a small utility found in most Unix-like systems) 
# as a data server by using
#
# $ nc -lk 9999


# Second, in a different terminal, you can start the example by using
#
# $ spark-submit structured_network_wordcount.py localhost 9999

# Finally, in the terminal running Netcat, write down some lines of text


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()



# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split the lines into words
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()

# Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()




