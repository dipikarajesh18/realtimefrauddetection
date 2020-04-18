import csv
from time import sleep 
from json import dumps
from kafka import KafkaProducer
# from pyspark.sql import SparkSession
# from pyspark import SparkContext
# from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition, KafkaRDD, OffsetRange
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# from pyspark.sql.functions import col, from_json
# from pyspark.sql.functions import to_json, col, struct


def spark_batch_produce():
	spark=SparkSession.builder.appName("SparkPublishfail").getOrCreate()

	data2=[[1,"James ","","Smith",2018,1,"M",3000],
		   [2,"Michael ","Rose","",2010,3,"M",4000],
		   [3,"Robert ","","Williams",2010,3,"M",4000]
		   [4,"Maria ","Anne","Jones",2005,5,"F",4000],
		   [5,"Jen ","Mary","Brown",2010,7,"",-1]

		   ]

	columns=["id","firstname","middlename","lastname","dob_year","dob_month","gender","salary"]

	df2=data2.toDF(*columns)

	df2.toJSON\
		.write\
		.format("kafka")\
		.option("kafka.bootstrap_servers","localhost:9092")\
		.option("topic","useless_topic")


producer=KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x:dumps(x).encode('utf-8'))

with open('creditcard.csv') as csv_file:
	csv_reader=csv.reader(csv_file)
	line_count=0
	for row in csv_reader:
		if(line_count ==0):
			print(f'Columns are :{",".join(row)}')
			line_count+=1
		else:
			print(f'\t Publishing to kafka topic, Amount:{row[29]} ')
			line_count+=1
		data={}
		data['Time']=row[0]
		data['V1']=row[1]
		data['V2']=row[2]
		data['V3']=row[3]
		data['V4']=row[4]
		data['V5']=row[5]
		data['V6']=row[6]
		data['V7']=row[7]
		data['V8']=row[8]		
		data['V9']=row[9]
		data['V10']=row[10]
		data['V11']=row[11]
		data['V12']=row[12]
		data['V13']=row[13]
		data['V14']=row[14]
		data['V15']=row[15]
		data['V16']=row[16]
		data['V17']=row[17]
		data['V18']=row[18]
		data['V19']=row[19]
		data['V20']=row[20]
		data['V21']=row[21]
		data['V22']=row[22]
		data['V23']=row[23]
		data['V24']=row[24]
		data['V25']=row[25]
		data['V26']=row[26]
		data['V27']=row[27]
		data['V28']=row[28]
		data['Amount']=row[29]
		# data['Class']=row[30]
		producer.send('useless_topic',value=data)
		sleep(1)

	print(f'Processed {line_count} lines.')
# tempSchema=StructType([
# 		StructField('id',IntegerType(),True),
# 		StructField('firstname',StringType(),True),
# 		StructField('middlename',StringType(),True),
# 		StructField('lastname',StringType(),True),
# 		StructField('dob_year',IntegerType(),True),
# 		StructField('dob_month',IntegerType(),True),
# 		StructField('gender',StringType(),True),
# 		StructField('salary',IntegerType(),True)
# 		])

# spark=SparkSession.builder.appName("SparkPublishfail").getOrCreate()

# data2=[
# 		(1,"James ","","Smith",2018,1,"M",3000),
# 		(2,"Michael ","Rose","",2010,3,"M",4000),
# 		(3,"Robert ","","Williams",2010,3,"M",4000),
# 		(4,"Maria ","Anne","Jones",2005,5,"F",4000),
# 		(5,"Jen ","Mary","Brown",2010,7,"",-1)
# 	]
# columns=["id","firstname","middlename","lastname","dob_year","dob_month","gender","salary"]

# # df2=data2.toDF(*columns)
# df2=spark.createDataFrame(data2,tempSchema)
# df2.printSchema()
# df2.show()

# df2.write\
# 	.format("kafka")\
# 	.option("kafka.bootstrap_servers","localhost:9092")\
# 	.option("topic","new_useless")

# df2.select(to_json(struct([col(c).alias(c) for c in df2.columns])))\
# 	.write\
# 	.format("kafka")\
# 	.option("kafka.bootstrap.servers", "localhost:9092")\
# 	.option("topic", "new_useless")\
# 	.save()


# ds = df2 \
#   .writeStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "localhost:9092") \
#   .option("topic", "new_useless") \
#   .start()

# df2\
# 	.write \
# 	.format("kafka") \
# 	.option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
# 	.option("topic", "topic1") \
# 	.save()

# query = df \
#   .selectExpr("CAST(userId AS STRING) AS key", "to_json(struct(*)) AS value") \
#   .writeStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
#   .option("topic", "topic1") \
#   .option("checkpointLocation", "/path/to/HDFS/dir") \
#   .start()