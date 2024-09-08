from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

spark = SparkSession.builder.appName("MovieDataAnalysis").getOrCreate()

df = spark.read.csv("netflix_titles.csv", header=True, inferSchema=True)
df.show(5)

df_clean = df.dropna()
df_clean.show()

df_clean.printSchema()

#Пробую фильтр
recent_movies = df_clean.filter(col('release_year') > 2010)
recent_movies.show()

#Пробую делать анализ
film_num_by_countries = df_clean.groupBy('country', 'release_year').agg(count('*').alias('film_count'))
film_num_by_countries.show()

film_num_by_genre = df_clean.groupBy('type').agg(count('*').alias('movie_count'))
film_num_by_genre.show()

oldest_movies = df_clean.orderBy('release_year', ascending=True).limit(5)
oldest_movies.show()

#можно вывести среднее число

oldest_movies.write.csv("/Users/temirlantursynbekov/Desktop/oldest_movies.csv", header=True)
oldest_movies.write.csv("/Users/temirlantursynbekov/Desktop/oldest_movies_output", header=True)

