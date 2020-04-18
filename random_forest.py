from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from time import *
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from sklearn.metrics import classification_report, confusion_matrix

from imblearn.over_sampling import SMOTE
from imblearn.combine import SMOTEENN
from sklearn.model_selection import train_test_split

import pandas as pd
import numpy as np

from collections import Counter

conf=SparkConf()
conf.set("spark.executor.memory", "4g")
conf.set("spark.driver.memory", "4g")
conf.set("spark.cores.max", "2")


#Start the spark session and initialize the context
spark = SparkSession.builder.appName("final_project").getOrCreate()
spark


#Load the CSV data into a dataframe and keep track of feature columns
CSV_PATH = "creditcard.csv"
df = spark.read.csv(CSV_PATH, inferSchema=True, header=True)
columns = [i for i in df.columns if i!='Class']

#Convert the spark dataframe into Pandas for applying SMOTE - oversampling method
imputeDF = df
imputeDF_Pandas = imputeDF.toPandas()

#Split the dataframe into features (X) and labels (Y) format followed by pandas
X = imputeDF.toPandas().filter(items=columns)
Y = imputeDF.toPandas().filter(items=['Class'])
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.1, random_state=0)

#Apply oversampling method SMOTE to the dataset
sm = SMOTE(random_state=0)
x_train_res, y_train_res = sm.fit_sample(X_train, Y_train)
# print('Resampled dataset shape {}'.format(Counter(y_train_res['Class'])))

#Format the dataset into SparkDataframe format to continue processing with Spark
dataframe_1 = pd.DataFrame(x_train_res,columns=columns)
dataframe_2 = pd.DataFrame(y_train_res, columns = ['Class'])
result = dataframe_1.combine_first(dataframe_2)
imputeDF_1 = spark.createDataFrame(result)

#Assemble the features using VectorAssembler
assembler = VectorAssembler(inputCols= columns, outputCol='features')
assembled = assembler.transform(imputeDF_1)
# print("assembled ",assembled)

#Split the dataset into Training and Testing Data with a 70-30 split
(trainingData, testData) = assembled.randomSplit([0.7, 0.3], seed=0)

#Create an instance of Random Forest Classifier from Spark ML Library
algo = RandomForestClassifier(featuresCol='features', labelCol='Class')

#Train the model
start_time = time()
model = algo.fit(trainingData)
end_time = time()

print("Time to train model: %.3f seconds" % (end_time - start_time))

#Create predictions using the Test Dataset
predictions = model.transform(testData)
predictions.select(['Class','prediction', 'probability']).show()

#Evaluate the model 
evaluator = BinaryClassificationEvaluator(labelCol='Class', metricName='areaUnderROC')
evaluator.evaluate(predictions)

#Use Area Under ROC evaluation metric
print("Test: Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))

#Create confusion matrix
y_true = predictions.select(['Class']).collect()
y_pred = predictions.select(['prediction']).collect()
print(classification_report(y_true, y_pred))

#Save the weights in the model to use this trained model for predictions
model.write().overwrite().save("randomforestmodel_saved")