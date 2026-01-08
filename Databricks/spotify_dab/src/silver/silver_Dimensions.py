# Databricks notebook source
# MAGIC %md
# MAGIC ## **DimUser**

# COMMAND ----------

# MAGIC %md
# MAGIC #### **AUTOLOADER**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys
project_path = os.path.join(os.getcwd(), "..", "..")
sys.path.append(project_path)

from utils.transformations import reusable

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles") \
                .option("cloudFiles.format", "parquet") \
                .option("cloudFiles.schemaLocation", "abfss://silver@azurespotifyproject.dfs.core.windows.net/DimUser/checkpoint") \
                .option("cloudFiles.schemaEvolutionMode", "rescue") \
                .load("abfss://bronze@azurespotifyproject.dfs.core.windows.net/DimUser")

# COMMAND ----------

df = df_user.withColumn("user_name", upper(col("user_name")))

# COMMAND ----------

df_user_obj = reusable()

df_user = df_user_obj.dropColumns(df_user, ['_rescued_data'])
display(df_user_obj)

# COMMAND ----------

df_user = df_user.dropDuplicates(['user_id'])

# COMMAND ----------

df_user.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "abfss://silver@azurespotifyproject.dfs.core.windows.net/DimUser/checkpoint") \
        .trigger(once=True) \
        .option("path", "abfss://silver@azurespotifyproject.dfs.core.windows.net/DimUser/data") \
        .toTable("spotify_catalog.silver.DimUser")


# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimArtist**

# COMMAND ----------

df_art = spark.readStream.format("cloudFiles") \
                .option("cloudFiles.format", "parquet") \
                .option("cloudFiles.schemaLocation", "abfss://silver@azurespotifyproject.dfs.core.windows.net/DimArtist/checkpoint") \
                .option("cloudFiles.schemaEvolutionMode", "rescue") \
                .load("abfss://bronze@azurespotifyproject.dfs.core.windows.net/DimArtist")

# COMMAND ----------

df_art_obj = reusable()

df_art = df_art_obj.dropColumns(df_art, ['_rescued_data'])
df_art = df_art.dropDuplicates(['artist_id'])


# COMMAND ----------

df_art.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "abfss://silver@azurespotifyproject.dfs.core.windows.net/DimArtist/checkpoint") \
        .trigger(once=True) \
        .option("path","abfss://silver@azurespotifyproject.dfs.core.windows.net/DimArtist/data") \
        .toTable("spotify_catalog.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimTrack**

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles") \
                .option("cloudFiles.format", "parquet") \
                .option("cloudFiles.schemaLocation", "abfss://silver@azurespotifyproject.dfs.core.windows.net/DimTrack/checkpoint") \
                .option("cloudFiles.schemaEvolutionMode", "rescue") \
                .load("abfss://bronze@azurespotifyproject.dfs.core.windows.net/DimTrack")

# COMMAND ----------

df_track = df_track.withColumn("durationFlag", when(col("duration_sec") < 150, "low") \
                                    .when(col("duration_sec") < 300, "medium") \
                                    .otherwise("high"))

df_track = df_track.withColumn("track_name", regexp_replace(col("track_name"), "-", " "))


df_track = reusable().dropColumns(df_track, ['_rescued_data'])


# COMMAND ----------

df_track.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "abfss://silver@azurespotifyproject.dfs.core.windows.net/DimTrack/checkpoint") \
        .trigger(once=True) \
        .option("path","abfss://silver@azurespotifyproject.dfs.core.windows.net/DimTrack/data") \
        .toTable("spotify_catalog.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimDate**

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles") \
                .option("cloudFiles.format", "parquet") \
                .option("cloudFiles.schemaLocation", "abfss://silver@azurespotifyproject.dfs.core.windows.net/DimDate/checkpoint") \
                .option("cloudFiles.schemaEvolutionMode", "rescue") \
                .load("abfss://bronze@azurespotifyproject.dfs.core.windows.net/DimDate")

# COMMAND ----------

df_date = reusable().dropColumns(df_date, ['_rescued_data'])


# COMMAND ----------

df_date.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "abfss://silver@azurespotifyproject.dfs.core.windows.net/DimDate/checkpoint") \
        .trigger(once=True) \
        .option("path","abfss://silver@azurespotifyproject.dfs.core.windows.net/DimDate/data") \
        .toTable("spotify_catalog.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **FactStream**

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles") \
                .option("cloudFiles.format", "parquet") \
                .option("cloudFiles.schemaLocation", "abfss://silver@azurespotifyproject.dfs.core.windows.net/FactStream/checkpoint") \
                .option("cloudFiles.schemaEvolutionMode", "rescue") \
                .load("abfss://bronze@azurespotifyproject.dfs.core.windows.net/FactStream")

# COMMAND ----------

df_fact = reusable().dropColumns(df_fact, ['_rescued_data'])

# COMMAND ----------

df_fact.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "abfss://silver@azurespotifyproject.dfs.core.windows.net/FactStream/checkpoint") \
        .trigger(once=True) \
        .option("path","abfss://silver@azurespotifyproject.dfs.core.windows.net/FactStream/data") \
        .toTable("spotify_catalog.silver.FactStream")