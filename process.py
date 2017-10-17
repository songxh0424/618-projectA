from pyspark import SparkContext
from pyspark.sql import SQLContext
import os
import re

################################################################################
## register tables
################################################################################
sc = SparkContext(appName = "movies")
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

filenames = os.listdir('./data')

## load all json files into dataframes and register as tables
for fn in filenames:
    df = sqlContext.read.json('file:///home/songxh/si618_midterm_project/data/' + fn)
    tbname = fn.replace('movie_', '')[:-5]
    df.registerTempTable(tbname)

df = sqlContext.read.json('file:///home/songxh/si618_midterm_project/data/user_ratedmovies-timestamps.json')
df.registerTempTable(user_ratings)

## Table names: movies, tags, locations, genres, directors, countries, actors, tags_map, user_ratings

## Columns of interests:
## movies: id, title, year, rtAllCriticsRating, rtAllCriticsNumReviews, rtAllCriticsNumFresh, rtAllCriticsNumRotten, rtAllCriticsScore, rtTopCriticsRating,
##         rtTopCriticsNumReviews, rtTopCriticsNumFresh, rtTopCriticsNumRotten, rtTopCriticsScore, rtAudienceRating, rtAudienceNumRatings, rtAudienceScore
## movie_countries: movieID, country (not very important)
## movie_genres: movieID, genre
## movie_actors: movieID, actorID, actorName, ranking
## movie_directors: movieID, directorID, directorName
## movie_locations: movieID, location1, location2, location3, location4 (least important one)
## movie_tags: movieID, tagID, tagWeight
## tags_map: id, value
## user_ratings: userID, movieID, rating


################################################################################
## Tasks
################################################################################
