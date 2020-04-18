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

spark = SparkSession.builder.appName("final_project").getOrCreate()
spark

CSV_PATH = "creditcard.csv"
df = spark.read.csv(CSV_PATH, inferSchema=True, header=True)
columns = [i for i in df.columns if i!='Class']

print("loaded data")

imputeDF = df
imputeDF_Pandas = imputeDF.toPandas()

X = imputeDF.toPandas().filter(items=columns)
Y = imputeDF.toPandas().filter(items=['Class'])
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.1, random_state=0)

print("oversampling")
sm = SMOTE(random_state=0)
x_train_res, y_train_res = sm.fit_sample(X_train, Y_train)
print('Resampled dataset shape {}'.format(Counter(y_train_res['Class'])))
print("oversampling finished")

dataframe_1 = pd.DataFrame(x_train_res,columns=columns)
dataframe_2 = pd.DataFrame(y_train_res, columns = ['Class'])
# frames = [dataframe_1, dataframe_2]
result = dataframe_1.combine_first(dataframe_2)

print("converting back to spark df")
imputeDF_1 = spark.createDataFrame(result)
print("converted back to spark df")

assembler = VectorAssembler(inputCols= columns, outputCol='features')
assembled = assembler.transform(imputeDF_1)
print("assembled ",assembled)

(trainingData, testData) = assembled.randomSplit([0.7, 0.3], seed=0)
# trainingData.cache()
# testData.cache()
# print('Distribution of Ones and Zeros in trainingData is: ', trainingData.groupBy('Class').count().take(3))

algo = RandomForestClassifier(featuresCol='features', labelCol='Class')

print("start training")
start_time = time()
model = algo.fit(trainingData)
end_time = time()
print("trained")

print("Time to train model: %.3f seconds" % (end_time - start_time))

predictions = model.transform(testData)
predictions.select(['Class','prediction', 'probability']).show()

evaluator = BinaryClassificationEvaluator(labelCol='Class', metricName='areaUnderROC')
evaluator.evaluate(predictions)
print("Test: Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))

y_true = predictions.select(['Class']).collect()
y_pred = predictions.select(['prediction']).collect()
print(classification_report(y_true, y_pred))

model.write().overwrite().save("randomforestmodel_saved")