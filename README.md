# Movie Data Analysis with PySpark
This project uses PySpark to analyze movie data from a CSV file containing Netflix titles. The analysis includes filtering recent movies, counting films by country and genre, and identifying the oldest movies in the dataset.

## Project Setup
### Prerequisites
Apache Spark (version compatible with PySpark)

Python (version 3.6 or later)

PySpark (install via pip)

## Installation
Install PySpark:

```bash
pip install pyspark
```
Download Dataset: Ensure you have the netflix_titles.csv file in your working directory. You can obtain this file from the Netflix dataset.

## Code Description
### 1. Setup and Initialization:

```
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

spark = SparkSession.builder.appName("MovieDataAnalysis").getOrCreate()
```
Initializes a Spark session for the application.

### 2. Read CSV Data:

```
df = spark.read.csv("netflix_titles.csv", header=True, inferSchema=True)
df.show(5)
```
Reads the Netflix titles CSV file into a DataFrame and displays the first 5 rows.

### 3. Data Cleaning:

```
df_clean = df.dropna()
df_clean.show()
```
Drops rows with missing values and displays the cleaned DataFrame.

### 4. Filtering Recent Movies:

```
recent_movies = df_clean.filter(col('release_year') > 2010)
recent_movies.show()
```
Filters the DataFrame to include only movies released after 2010 and displays them.

### 5. Analyzing Film Counts:
```
film_num_by_countries = df_clean.groupBy('country', 'release_year').agg(count('*').alias('film_count'))
film_num_by_countries.show()

film_num_by_genre = df_clean.groupBy('type').agg(count('*').alias('movie_count'))
film_num_by_genre.show()
```

Groups and counts films by country and release year.

Groups and counts films by genre/type.

### 6. Identifying Oldest Movies:

```
oldest_movies = df_clean.orderBy('release_year', ascending=True).limit(5)
oldest_movies.show()
```
Orders movies by release year and retrieves the 5 oldest movies.

### 7. Saving Results:

```
oldest_movies.write.csv("/Users/temirlantursynbekov/Desktop/oldest_movies.csv", header=True)
oldest_movies.write.csv("/Users/temirlantursynbekov/Desktop/oldest_movies_output", header=True)
```
Saves the DataFrame of oldest movies to a CSV file.
