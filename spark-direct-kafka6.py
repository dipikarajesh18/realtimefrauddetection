import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition, KafkaRDD, OffsetRange
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import col, from_json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import to_json, col, struct
from pyspark.sql.functions import split
from pyspark.ml.feature import CountVectorizer, IDF, Tokenizer, RegexTokenizer, StopWordsRemover, IDF, MinHashLSH
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.tree import RandomForest
from pyspark.ml.classification import RandomForestClassifier
from time import *
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from time import *


offsetRanges = []

def storeOffsetRanges(rdd):
	global offsetRanges
	offsetRanges = rdd.offsetRanges()
	return rdd

def printOffsetRanges(rdd):
	for o in offsetRanges:
		print "#########%s %s %s %s###########" % (o.topic, o.partition, o.fromOffset, o.untilOffset)

def process_dstream(rdd):
	rdd.foreachPartition(lambda iter: do_some_work(iter))
	krdd=KafkaRDD(rdd._jrdd,sc,rdd._jrdd_deserializer)
	off_ranges=krdd.offsetRanges()

	for o in off_ranges:
		x='###################'+str(o)+"#############"
		print(x)

def process(time, rdd):
    print("========= %s =========" % str(time))

    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w: Row(word=w))
        # rowRdd.pprint()
        wordsDataFrame = spark.createDataFrame(rowRdd)
        # wordsDataFrame.printSchema()
        print(wordsDataFrame)
        wordsDataFrame.write.format("csv").save("/Users/prasannasurianarayanan/Desktop/dfstore.csv")
        # Creates a temporary view using the DataFrame.
        wordsDataFrame.createOrReplaceTempView("words")

        # Do word count on table using SQL and print it
        wordCountsDataFrame = \
            spark.sql("select word, count(*) as total from words group by word")
        # wordCountsDataFrame.show()
    except:
        pass


if __name__ == "__main__":

	spark=SparkSession.builder.appName("SparkPublishfail").getOrCreate()

	kafkaTransactionSchema=StructType([
	StructField('cc_num',StringType(),True),
	StructField('first',StringType(),True)
	])

	kafkaCreditCardSchema=StructType([
	StructField('Time',StringType(),True),
	StructField('V1',StringType(),True),
	StructField('V2',StringType(),True),
	StructField('V3',StringType(),True),
	StructField('V4',StringType(),True),
	StructField('V5',StringType(),True),
	StructField('V6',StringType(),True),
	StructField('V7',StringType(),True),
	StructField('V8',StringType(),True),
	StructField('V9',StringType(),True),
	StructField('V10',StringType(),True),
	StructField('V11',StringType(),True),
	StructField('V12',StringType(),True),
	StructField('V13',StringType(),True),
	StructField('V14',StringType(),True),
	StructField('V15',StringType(),True),
	StructField('V16',StringType(),True),
	StructField('V17',StringType(),True),
	StructField('V18',StringType(),True),
	StructField('V19',StringType(),True),
	StructField('V20',StringType(),True),
	StructField('V21',StringType(),True),
	StructField('V22',StringType(),True),
	StructField('V23',StringType(),True),
	StructField('V24',StringType(),True),
	StructField('V25',StringType(),True),
	StructField('V26',StringType(),True),
	StructField('V27',StringType(),True),
	StructField('V28',StringType(),True),
	StructField('Amount',StringType(),True),
	# StructField('Class',DoubleType(),True)
	
])

	nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"

	jsonOptions = { "timestampFormat": nestTimestampFormat }
	parsed = spark \
	.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers", "localhost:9092") \
	.option("subscribe", "useless_topic") \
	.load() 
	parsed=parsed.select(col("key").cast("string"),from_json(col("value").cast("string"), kafkaCreditCardSchema))
	parsed.printSchema() 
	
	newFields = parsed \
		.select("jsontostructs(CAST(value AS STRING)).Time",
				"jsontostructs(CAST(value AS STRING)).V1",	
				"jsontostructs(CAST(value AS STRING)).V2",
				"jsontostructs(CAST(value AS STRING)).V3",
				"jsontostructs(CAST(value AS STRING)).V4",
				"jsontostructs(CAST(value AS STRING)).V5",
				"jsontostructs(CAST(value AS STRING)).V6",
				"jsontostructs(CAST(value AS STRING)).V7",
				"jsontostructs(CAST(value AS STRING)).V8",
				"jsontostructs(CAST(value AS STRING)).V9",
				"jsontostructs(CAST(value AS STRING)).V10",
				"jsontostructs(CAST(value AS STRING)).V11",
				"jsontostructs(CAST(value AS STRING)).V12",
				"jsontostructs(CAST(value AS STRING)).V13",
				"jsontostructs(CAST(value AS STRING)).V14",
				"jsontostructs(CAST(value AS STRING)).V15",
				"jsontostructs(CAST(value AS STRING)).V16",
				"jsontostructs(CAST(value AS STRING)).V17",
				"jsontostructs(CAST(value AS STRING)).V18",
				"jsontostructs(CAST(value AS STRING)).V19",
				"jsontostructs(CAST(value AS STRING)).V20",
				"jsontostructs(CAST(value AS STRING)).V21",
				"jsontostructs(CAST(value AS STRING)).V22",
				"jsontostructs(CAST(value AS STRING)).V23",
				"jsontostructs(CAST(value AS STRING)).V24",
				"jsontostructs(CAST(value AS STRING)).V25",
				"jsontostructs(CAST(value AS STRING)).V26",
				"jsontostructs(CAST(value AS STRING)).V27",
				"jsontostructs(CAST(value AS STRING)).V28",
				"jsontostructs(CAST(value AS STRING)).Amount"
			)

	model = RandomForestClassifier.load('randomforestmodel_saved')
	predictions = model.predict(newFields.rdd.map(lambda x: x.features))
	
	#write predictions to output sink fraud topic
	# read from the topic directly using the console to view the predictions
	query = predictions \
			.writeStream \
			.format("kafka") \
			.option("publish","fraud_topic")
			.start()
	query.awaitTermination()

	newFields.printSchema()
	print('count of dataframe : ',newFields.count())
	newFields.show()
	

