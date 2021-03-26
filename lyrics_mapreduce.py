#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from operator import add

# New API
spark_session = SparkSession        .builder        .master("spark://192.168.2.111:7077")         .appName("lyrics_mapreduce")        .config("spark.dynamicAllocation.executorIdleTimeout","30s")        .config("spark.executor.cores", 4)        .config("spark.driver.port",9998)        .config("spark.blockManager.port",10005)        .getOrCreate()

# Old API (RDD)
spark_context = spark_session.sparkContext

spark_context.setLogLevel("INFO")


# In[2]:


import pyspark.sql.functions as F

#Get most frequent words for each song ID
lyrics = spark_session.read           .option("header", "true")           .csv("hdfs://192.168.2.111:9000/user/ubuntu/lyrics_database.csv")            .cache()

#Get genre tags for each song ID
lastfm = spark_session.read           .json("hdfs://192.168.2.111:9000/user/ubuntu/lastfm/lastfm_test/*/*/*")            .repartition(30)           .cache()

lyrics.count()


# In[ ]:





# In[ ]:


from pyspark.sql.types import *

#Filter out irrelevant attributes in genre dataset
genre = lastfm.filter(F.size(lastfm["tags"]) > 0 )        .select("tags", "track_id")        .cache()

genre.count()


# In[4]:


#Look at data

lyrics.show(3)
genre.show(4)
genre.printSchema()
lyrics.printSchema()


# In[5]:


#Join both datasets on their ID
paired_songs = lyrics.join(genre, "track_id").cache()
paired_songs.show(4)

paired_songs.count()


# In[6]:


from stop_words import get_stop_words

#Filter stopwords from the frequent words
stopwords = get_stop_words("english")
#ONLY RUN ONCE - For more interesting results
# stopwords.append("just")
# stopwords.append("will")

#Create new DataFrame with a column recasting count to integer
songs_int_count = paired_songs.filter(paired_songs['word'].isin(stopwords)==False)                        .withColumn("wordcount", songs_expanded["count"].cast(IntegerType()))                        .drop("count")
#Create new DataFrame that contains one row for each genre tag, the word, and the word count in each song
songs_expanded = songs_int_count.select("word",                                "wordcount",                                F.explode(paired_songs["tags"]))                        .withColumnRenamed("col","genre")                        .cache()
    
songs_expanded.show(3)


# In[7]:


import pyspark.sql.types

#Remove second element in tuple to obtain only genre tag
def remove_similarity(genre_tuple):
    genre, _ = genre_tuple
            
    return genre

tags_function = F.udf(remove_similarity, StringType())

#Cast count to an integer type and remove second element in genre tuple 
wordcount_genre = songs_expanded.withColumn("genre", tags_function(songs_expanded["genre"]))                            .cache()

wordcount_genre.show(2)


# In[8]:


# Group elements with common genre and word, and sum their wordcounts
wordcount_genre.groupBy("word", "genre")                .agg(F.sum("wordcount"))                .sort("sum(wordcount)",ascending=False)                .show()


# In[9]:


#Group elements by genre and word, to see which pairs are the most frequent
wordcount_genre.groupBy("genre", "word")                .count()                .sort("count", ascending=False)                .show(40)


# In[10]:


# To find most common genres

wordcount_genre.groupBy("genre")                .count()                .sort("count", ascending=False)                .show()


# In[11]:



#Print lists of most common words for the top 5 most common genre tags
top_genres = {"rock", "pop", "alternative", "indie", "Hip-Hop"}
genre_top_words = []

for genre in top_genres:
    #genre_top_words += 
    wordcount_genre.filter(wordcount_genre["genre"] == genre)            .groupBy("genre", "word")            .agg(F.sum("wordcount"))            .sort("sum(wordcount)", ascending=False)            .limit(10)            .show()


# In[3]:


spark_context.stop()


# In[ ]:



import pandas as pd
import matplotlib.pyplot as plt

#print(genre_top_words)

#df = pd.DataFrame({'word': ['word1', 'word2'], 'count': [12898, 4861]})
#df.plot.bar(x='word', y='count', rot=0)


# In[3]:


df = spark_session.read.format('jdbc').options(url='hdfs://master:9000/user/ubuntu/jdbc:sqlite:mxm_dataset.db',dbtable='lyrics',driver='org.sqlite.JDBC').load()

df.show(2)


# In[ ]:




