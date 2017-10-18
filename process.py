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

filenames = os.listdir('./movie-scores')

## load all json files into dataframes and register as tables
for fn in filenames:
    df = sqlContext.read.json('file:///home/songxh/618-projectA/movie-scores/' + fn)
    if fn == 'movies.json':
        df = df.filter(df.year >= 1930).filter(df.year < 2010)
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
       AVG(rtAudienceRating) AS rtAudRating, AVG(rtAudienceScore) AS rtAudScore,
       AVG(imdbRating) AS imdbRating, AVG(Metascore) AS metascore
  FROM movies
  GROUP BY year
  ORDER BY year
"""
trends = sqlContext.sql(sql_cmd).collect()

# 2.actors/directors with highest average scores, 3 movies or more
# can also look at the actor/director duos, like Johnny Depp and Tim Burton
sql_meta = """
SELECT md.directorName, AVG(m.Metascore) AS Metascore, COUNT(m.id) AS count
  FROM movies AS m JOIN directors AS md ON m.id = md.movieID
  WHERE m.Metascore != 'N/A'
  GROUP BY md.directorName
  HAVING COUNT(m.id) >= 3
  ORDER BY Metascore DESC
"""
topD_meta = sqlContext.sql(sql_meta).collect()

sql_imdb = """
SELECT md.directorName, AVG(m.imdbRating) AS imdbRating, COUNT(m.id) AS count
  FROM movies AS m JOIN directors AS md ON m.id = md.movieID
  WHERE m.imdbRating != 'N/A'
  GROUP BY md.directorName
  HAVING COUNT(m.id) >= 3
  ORDER BY imdbRating DESC
"""
topD_imdb = sqlContext.sql(sql_imdb).collect()

sql_tomato = """
SELECT md.directorName, AVG(m.rtAllCriticsScore) AS Tomatometer, COUNT(m.id) AS count
  FROM movies AS m JOIN directors AS md ON m.id = md.movieID
  WHERE m.rtAllCriticsNumReviews != 0
  GROUP BY md.directorName
  HAVING COUNT(m.id) >= 3
  ORDER BY Tomatometer DESC
"""
topD_tomato = sqlContext.sql(sql_tomato).collect()

sql_rtAud = """
SELECT md.directorName, AVG(m.rtAudienceScore) AS rtAudScore, COUNT(m.id) AS count
  FROM movies AS m JOIN directors AS md ON m.id = md.movieID
  WHERE m.rtAudienceNumRatings != 0
  GROUP BY md.directorName
  HAVING COUNT(m.id) >= 3
  ORDER BY rtAudScore DESC
"""
topD_rtAud = sqlContext.sql(sql_rtAud).collect()
