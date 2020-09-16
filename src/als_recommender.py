import pyspark as ps
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from sklearn.metrics import mean_squared_error
spark = SparkSession.builder.getOrCreate()

data = pd.read_csv('../data/filtered_ratings.csv')
data = data.iloc[:1000000,:4]
ratings_df = spark.createDataFrame(data)
train, test = ratings_df.randomSplit([0.8, 0.2], seed=51)
print(train.count())
# density of my training data
# num_ratings/ (num_users * num_movies)
num_ratings = train.count()
num_users = train.select("userId").distinct().count()
num_movies = train.select("movieId").distinct().count()

density = num_ratings/ (num_users * num_movies)
print(f'density: {density}')

# create an untrained ALS factorization model.
from pyspark.ml.recommendation import ALS
als_model = ALS(
    itemCol='movieId',
    userCol='userId',
    ratingCol='rating',
    nonnegative=True,
    coldStartStrategy="drop",
    regParam=0.2) 

recommender = als_model.fit(train)

predictions = recommender.transform(test)
print(predictions.show())
predictions_pd = predictions.toPandas()

y = np.array(predictions_pd.rating.values)
y_hat = np.array(predictions_pd.prediction.values)
MSE = mean_squared_error(y, y_hat)
RMSE = np.sqrt(MSE)
print(MSE, RMSE)
