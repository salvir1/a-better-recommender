import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from lxml import html
import pickle
import json
import pandas as pd
import random
import re
import time

keys = pd.read_csv('keys.txt', header=None)
api_key = keys.iloc[0,1]

class Movie:
    '''class that stores data for a movie'''
    def __init__(
        self,
        title: str = '',
        poster_path: str = '',
        movielensId: str = '',
        movielens_mean_rating: float = None,
        movielens_std_rating: float = None,
        tmdbId: str = '',
        imdbId: str = '',
        budget: int = None,
        revenue: int = None,
        tmdb_popularity: float = None,
        tmdb_vote_average: float = None,
        tmdb_vote_count: int = None,
        release_date: str = '',
        tmdb_overview: str = '',
        tmdb_original_ln: str = '',
        cast: list = [],
        tmdb_genre_1: str = '',
        tmdb_genre_2: str = '',
        tmdb_genre_3: str = '',
        tmdb_recommendations: list = []
    ):
        
        self.title = title,
        self.poster_path = poster_path,
        self.movielensId = movielensId,
        self.movielens_mean_rating = movielens_mean_rating,
        self.movielens_std_rating = movielens_std_rating,
        self.tmdbId = tmdbId,
        self.imdbId = imdbId,
        self.budget = budget,
        self.revenue = revenue,
        self.tmdb_popularity = tmdb_popularity,
        self.tmdb_vote_average = tmdb_vote_average,
        self.tmdb_vote_count = tmdb_vote_count,
        self.release_date = release_date,
        self.tmdb_overview = tmdb_overview,
        self.tmdb_original_ln = tmdb_original_ln,
        self.cast = cast,
        self.tmdb_genre_1 = tmdb_genre_1,
        self.tmdb_genre_2 = tmdb_genre_2,
        self.tmdb_genre_3 = tmdb_genre_3,
        self.tmdb_recommendations = tmdb_recommendations

    def load_tmdb_features(self, movielensId, movie_id):
        mv_url = f'https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key}&language=en-US'
        r_mv = requests.get(mv_url, allow_redirects=False)
        try:
            time.sleep(0.23)
            if r_mv.status_code == 200:
                pass
#             print(f"Success {r_mv.status_code}, {mv_url}")
            else:
                print(f"Fail {r_mv.status_code}, {mv_url}")

            mv_soup = BeautifulSoup(r_mv.content, features="lxml")
            dict = json.loads(mv_soup.get_text())
            self.title = dict['title']
            self.poster_path = dict['poster_path']
            self.movielensId = movielensId,
            self.tmdbId = movie_id,
            self.imdbId = dict['imdb_id'],
            self.budget = dict['budget'],
            self.revenue = dict['revenue'],
            self.tmdb_popularity = dict['popularity'],
            self.tmdb_vote_average = dict['vote_average'],
            self.tmdb_vote_count = dict['vote_count'],
            self.release_date = dict['release_date'],
            self.tmdb_overview = dict['overview'],
            self.tmdb_original_ln = dict['original_language'],
        
            if len(dict['genres']) > 0:
                self.tmdb_genre_1 = dict['genres'][0]['name']
            if len(dict['genres']) > 1:
                self.tmdb_genre_2 = dict['genres'][1]['name']
            if len(dict['genres']) > 2:
                self.tmdb_genre_3 = dict['genres'][2]['name']
        except Exception as msg:
            print(self.tmdbId, 'Exception', msg)

            # Load cast info
        try:
            time.sleep(0.4)
            cast_url = f'https://api.themoviedb.org/3/movie/{movie_id}/credits?api_key={api_key}&language=en-US'
            r_cast = requests.get(cast_url, allow_redirects=False)
            if r_cast.status_code == 200:
                pass
#             print(f"Success {r_cast.status_code}, {cast_url}")
            else:
                print(f"Fail {r_cast.status_code}, {cast_url}")

            cast_soup = BeautifulSoup(r_cast.content, features="lxml")
            cast_dict = json.loads(cast_soup.get_text())
            self.cast = cast_dict['cast'][:10]
        except Exception as msg:
            print(self.tmdbId, 'Exception', msg)

            #Load TMDB recommendations
        try:
            time.sleep(0.33)
            recs_url = f'https://api.themoviedb.org/3/movie/{movie_id}/recommendations?api_key={api_key}&language=en-US'
            r_recs = requests.get(recs_url, allow_redirects=False)
            if r_recs.status_code == 200:
                pass
#             print(f"Success {r_recs.status_code}, {recs_url}")
            else:
                print(f"Fail {r_recs.status_code}, {recs_url}")

            recs_soup = BeautifulSoup(r_recs.content, features="lxml")
            recs_dict = json.loads(recs_soup.get_text())
            self.tmdb_recommendations = recs_dict['results'][:20]
        except Exception as msg:
            print(self.tmdbId, 'Exception', msg)

    def load_movielens_features(self,mean_rating, std_rating):
        try: 
            self.movielens_mean_rating = mean_rating
            self.movielens_std_rating = std_rating
        except Exception as msg:
            print(self.tmdbId, 'Exception', msg)

    def convert_to_dict(self):
        return {
        'title' : self.title,
        'poster_path' : self.poster_path,
        'movielensId' : int(float('0'+self.movielensId[0])),
        'movielens_mean_rating' : self.movielens_mean_rating,
        'movielens_std_rating' : self.movielens_std_rating,
        'tmdbId' : self.tmdbId[0],
        'imdbId' : self.imdbId[0],
        'budget' : self.budget[0],
        'revenue' : self.revenue[0],
        'tmdb_popularity' : self.tmdb_popularity[0],
        'tmdb_vote_average' : self.tmdb_vote_average[0],
        'tmdb_vote_count' : self.tmdb_vote_count[0],
        'release_date' : pd.to_datetime(self.release_date[0]).year,
        'tmdb_overview' : self.tmdb_overview[0],
        'tmdb_original_ln' : self.tmdb_original_ln,
        'cast' : self.cast,
        'tmdb_genre_1' : self.tmdb_genre_1,
        'tmdb_genre_2' : self.tmdb_genre_2,
        'tmdb_genre_3' : self.tmdb_genre_3
        }
    
    def __repr__(self):
        return repr(self.__dict__)

    def __setitem__(self, key, item):
        self.__dict__[key] = item

    def __getitem__(self, key):
        return self.__dict__[key]
        
if __name__ == "__main__":
    # execute only if run as a script
    movie_list = pd.read_csv('../data/filtered_links.csv')

    movies = []
    for i in range(14600,len(movie_list)):
        time.sleep(random.randint(0,3))
        curr_movie = Movie()
        curr_movie.load_tmdb_features(str(movie_list.iloc[i][0]), str(movie_list.iloc[i][2]))
        curr_movie.load_movielens_features(str(movie_list.iloc[i][4]), str(movie_list.iloc[i][5]))
        movies.append(curr_movie)

        if i % 200 == 0:
            print(i, curr_movie.movielensId)
            with open('../data/mv_pkl.pkl','wb') as file:
                pickle.dump(movies, file)
