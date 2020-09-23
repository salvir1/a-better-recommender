''' Main program used by flask app to generate lists of 
recommended movies from a 'cold start' short list.
'''
import re
import numpy as np
import pandas as pd
import pickle
import boto3
from io import BytesIO
from lightfm import LightFM
from lightfm.data import Dataset

from pyspark.sql.functions import *
import pyspark as ps    # for the pyspark suite
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import IntegerType, StringType, FloatType, DateType, TimestampType
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext  # for the pre-2.0 sparkContext

s3 = boto3.client('s3')

def read_s3_csv_to_df(filename, bucket='galvrjsbucket'):
    '''reads in a csv file on s3 and returns a df'''

    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=filename)
    data = obj['Body'].read()
    f = BytesIO(data)
    df = pd.read_csv(f)
    return df

def assign_new_user_data(cold_start={933: 4.5,1035: 4}):
    '''Takes in cold_start dict of movies from a new user
        Returns pd dataframe
    '''
    new_user_data = pd.DataFrame.from_dict(cold_start, orient='index').reset_index()
    new_user_data = new_user_data.rename(columns={'index': "movieId", 0:"rating"})
    new_user_data['userId'] = 500000
    return new_user_data

def load_production_data(prod_s3_filename, new_user_data):
    '''takes in production data, adds new user data, and returns 
    a new combined df'''
    prod_data = read_s3_csv_to_df(prod_s3_filename)
    prod_data = prod_data.drop('Unnamed: 0', axis=1)
    prod_plus_new = prod_data.append(new_user_data)

    # Create a new user array of movies for the transform method
    new_user_for_pred = prod_data.groupby('movieId').agg(count=('rating','count')).reset_index()
    new_user_for_pred['userId'] = 500000
    new_user_for_pred = new_user_for_pred.sample(frac = 0.5)
    return prod_plus_new, new_user_for_pred

# def write_s3_pkl_file(user_scores):
#     read_s3_csv_to_df()
#     with open('../data/mv_pkl.pkl','wb') as file:
#         pickle.dump(movies, file)


class AlsRecs:
    def __init__(
        self,
        ordered_recs: list = []   
        ):
        
        self.ordered_recs = ordered_recs

    def load_recs(self, prod_plus_new, new_user_for_pred):
        prod_plus_new_spark = spark.createDataFrame(prod_plus_new)
        als = ALS(rank=20,
          maxIter=20,
          regParam=0.1,
          alpha=2,
          userCol="userId",
          itemCol="movieId",
          ratingCol="rating")

        ALS_fit_model = als.fit(prod_plus_new_spark)
        new_user_full_spark = spark.createDataFrame(new_user_for_pred)
        als_predictions = ALS_fit_model.transform(new_user_full_spark)
        als_predictions2 = als_predictions.orderBy('prediction', ascending=False).select('movieId').take(50)
        self.ordered_recs = [als_predictions2[i].movieId for i in range(len(als_predictions2))]


class LfmRecs:
    def __init__(
        self,
        ordered_recs: list = []   
        ):
        
        self.ordered_recs = ordered_recs

    def load_recs(self, prod_plus_new, new_user_for_pred):
        dataset2 = Dataset()
        dataset2.fit(prod_plus_new['userId'], prod_plus_new['movieId'])
        (interactions2, weights2) = dataset2.build_interactions([tuple(i) for i in prod_plus_new[['userId','movieId']].values])
        model = LightFM(learning_rate=0.027, 
                        no_components=23, 
                        loss='warp')
        model.fit(interactions2, 
                user_features=None, 
                epochs=56)
        prediction = model.predict(user_ids = dataset2.mapping()[0][new_user_for_pred['userId'].iloc[0]], item_ids =np.arange(len(new_user_for_pred)), item_features=None, user_features=None)
        lfm_movies = pd.DataFrame.from_dict(dataset2.mapping()[0], orient='index').reset_index()
        lfm_movies  = lfm_movies.rename(columns={'index': "movieId", 0:"to_drop"})
        lfm_movies = lfm_movies.drop('to_drop', axis=1).iloc[:len(prediction)]
        lfm_movies['prediction'] = prediction
        lfm_movies = lfm_movies.sort_values('prediction', ascending=False)
        self.ordered_recs = lfm_movies['movieId'][:50]


if __name__ == "__main__":
    # execute only if run as a script
    prod_data = read_s3_csv_to_df('prod_data.csv')
    rated_movies = {933: 4.5,
                1035: 4,
                922: 2,
                342: 5,
                2724: 1
               }
    new_user_data = assign_new_user_data(rated_movies)
    prod_new, new_user_pred = load_production_data('prod_data.csv', new_user_data)
    als = AlsRecs()
    # als.load_recs(prod_new, new_user_pred)
    # print(als.ordered_recs)
    lfm = LfmRecs()
    lfm.load_recs(prod_new, new_user_pred)
    print(lfm.ordered_recs)


