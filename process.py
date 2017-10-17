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
    df = sqlContext.read.json('file:///home/songxh/618-projectA/data/' + fn)
    df = sqlContext.read.json('/user/songxh/' + fn)
    tbname = fn.replace('movie_', '')[:-5]
    df.registerTempTable(tbname)

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
# 1.Trend over time
sql_cmd = """
SELECT year, AVG(rtAllCriticsRating) AS rtAllCritRating, AVG(rtTopCriticsRating) AS rtTopCritRating,
       AVG(rtAllCriticsScore) AS rtAllCritScore, AVG(rtTopCriticsScore) AS rtTopCritScore,
       AVG(rtAudienceRating) AS rtAudRating, AVG(rtAudienceScore) AS rtAudScore
  FROM movies
  GROUP BY year
  ORDER BY year
"""
trends = sqlContext.sql(sql_cmd).collect()

# 2.actors/directors with highest average scores, 3 movies or more
# can also look at the actor/director duos, like Johnny Depp and Tim Burton
sql_cmd = """
SELECT 
  FROM movies JOIN movie_actors AS ma ON movies.id = ma.movieID
