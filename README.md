Dataset used - https://www.kaggle.com/mlg-ulb/creditcardfraud

random_forest.py trains and saves the Random Forest Machine Learning model into the binary randomforestmodel_saved.

kafkaproducer.py is used to send data from csv file to the Kakfa Topics.  

spark-direct-kafka6.py is used to read the data from the Kafka Topics and preform the predictions using the trained and loaded Random Forest ML model from randomforestmodel_saved and to write the output prediction to sink. 

The predicted results can be seen in the Kafka Topic. 

(check out presentation.pdf for more details!)
