import csv
from time import sleep 
from json import dumps
from kafka import KafkaProducer

producer=KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x:dumps(x).encode('utf-8'))

#Create streaming instances using the CSV file
with open('creditcardsimulations.csv') as csv_file:
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
