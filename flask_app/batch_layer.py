from pyspark import SparkConf, SparkContext
import os
from pymongo import MongoClient
from pyspark.sql.functions import sum as _sum
from pyspark.sql import SparkSession
#import speed_layer


"""
TODO:
- Connect to MongoDB

    counts_df.write.format("mongo").mode("overwrite").option("spark.mongodb.output.uri", "mongodb://127.0.0.1/database.collection").save()
    spark = SparkSession.builder \
        .appName("batch_session") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()


- Without calling the batch init it with number 1 instead of doc_freq
- recalculate all tag clouds for each document
- create tag cloud for global TF-Sum with document frequency

"""

"""
hadoop
https://hadoop.apache.org/release/3.3.5.html

---- Start only Windows ---
hadoop binary
https://github.com/cdarlint/winutils/tree/master
replcae orginal bin folder with the downloaded bin folder
---- End  only Windows---

pip install pyspark
"""

def word_count():
    spark = SparkSession.builder \
            .appName("batch_session") \
            .getOrCreate()
    
    path = './doc_uploads'
    assert os.path.exists(path), f"Path {path} does not exist"
    text_files = spark.sparkContext.wholeTextFiles(path)

    counts = text_files.flatMap(lambda file: file[1].split(" ")) \
                   .map(lambda word: (word, 1)) \
                   .reduceByKey(lambda a, b: a + b)
    
    # Convert the counts RDD to a DataFrame
    counts_df = counts.toDF(["word", "count"])
    # Calculate sum of documents
    print("Counting and calculating document frequency...")
    total_documents = text_files.count()
    # ----------------------------------------------------------------------------
    # Calculate the document frequency
    counts_df = counts_df.withColumn("doc_freq", counts_df["count"] / total_documents)
    # ----------------------------------------------------------------------------
    counts_list = counts_df.rdd.map(lambda row: row.asDict()).collect()
    print("Word count processing completed successfully")

    # Create a MongoDB client
    client = MongoClient("mongodb://127.0.0.1:27017/")
    # Connect to your database
    db = client['db_doc_freq']
    # Connect to your collection
    collection = db['collection_doc_freq']
    # Insert the data into the collection
    collection.insert_many(counts_list)

    print("Batch to Mongo DB successfully")

    # Close the MongoDB connection
    client.close()

    spark.stop()

