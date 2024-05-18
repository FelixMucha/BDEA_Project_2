from pyspark import SparkConf, SparkContext
import os
from pymongo import MongoClient
from pyspark.sql.functions import sum as _sum
from pyspark.sql import SparkSession
import speed_layer
from pyspark.ml.feature import IDF, Tokenizer, CountVectorizer
from collections import Counter

"""
def calculate_inverse_document_frequency_spark(text, tf):
    # TODO: Implement the function to calculate inverse document frequency using Spark
    # TODO: try to do this with tf and spar instead of text
    return idf
"""
def calculate_inverse_document_frequency_spark(filenames, texts, tfs):
    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Create a DataFrame where each row is a document
    df = spark.createDataFrame([(t,) for t in texts], ["text"])

    # Tokenize the text
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    wordsData = tokenizer.transform(df)

    # Calculate term frequency
    cv = CountVectorizer(inputCol="words", outputCol="rawFeatures")
    cv_model = cv.fit(wordsData)
    featurizedData = cv_model.transform(wordsData)

    # Calculate IDF
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

    # Get the vocabulary (list of words)
    vocabulary = cv_model.vocabulary

    # Convert the features to a dictionary
    idf_dict = {word: idf for word, idf in zip(vocabulary, idfModel.idf.toArray())}

    # Stop the SparkSession
    spark.stop()

    return idf_dict


def read_all_data_from_mongo():
    # Connect to MongoDB
    client = MongoClient("mongodb://127.0.0.1:27017/")
    db = client['frequency_database']
    collection = db['frequency_scores']
    # Query all documents in the collection
    documents = collection.find()
    # Convert the documents to a list
    data = list(documents)
    # Close the MongoDB client
    client.close()
    return data

def update_idf_for_all_filenames(idf):
    # Connect to MongoDB
    client = MongoClient("mongodb://127.0.0.1:27017/")
    db = client['frequency_database']
    collection = db['frequency_scores']
    # Update the 'idf' field for all documents
    collection.update_many({}, {'$set': {'idf': idf}})
    print(f"IDF updated for all filenames")
    # Close the MongoDB client
    client.close()


def batch_job(cload_tag_folder):
    # read all files in mongo db
    data = read_all_data_from_mongo()
    # get all text data
    texts = [d['text'] for d in data]
    # get all filenames
    filenames = [d['filename'] for d in data]
    # get all tf data
    tfs = [d['tf'] for d in data]
    # calculate document frequency
    idf = calculate_inverse_document_frequency_spark(filenames, texts, tfs)
    # update idf in mongo db
    update_idf_for_all_filenames(idf)
    print("Document frequency calculation completed successfully")
    # for each file calculate tf-idf
    for filename, tf in zip(filenames, tfs):
        tf_idf = {word: tf[word] * idf[word] for word in tf if word in idf}
        # remove txt extension
        filename = filename.replace('.txt', '')
        # generate tag clouds
        speed_layer.generate_tag_cloud(tf_idf, filename, is_batch=True)

    # Calculate global TF sum
    global_tf = Counter()
    for tf in tfs:
        global_tf += Counter(tf)

    # Calculate TF-IDF for global TF
    global_tf_idf = {word: global_tf[word] * idf[word] for word in global_tf if word in idf}


    print(cload_tag_folder)
    # Get parent directory
    #cload_tag_folder = os.path.dirname(cload_tag_folder)
    #path_batch_cloud = os.path.join(cload_tag_folder, "batch")

    speed_layer.generate_tag_cloud(global_tf_idf, "batch", is_batch=True)
    print("batch Tag cloud generated successfully")

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