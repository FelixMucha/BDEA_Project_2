from pymongo import MongoClient
from pyspark.sql import SparkSession
import speed_layer
from pyspark.ml.feature import IDF, Tokenizer, CountVectorizer
from collections import Counter

def calculate_inverse_document_frequency_spark(texts):
    """
    Calculate the Inverse Document Frequency (IDF) for a collection of documents using Apache Spark.

    Args::
        texts: A list of document texts, where each element is the full text of a document.

    Returns:
        dict: A dictionary where the keys are terms and the values are their corresponding IDF scores.

    """

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

    
    spark.stop()

    return idf_dict


def read_all_data_from_mongo():
    """
    Connects to a MongoDB database and retrieves all documents from a specific collection.

    This function establishes a connection to a MongoDB instance running on localhost at the default port (27017).
    It then accesses the 'frequency_database' and retrieves all documents from the 'frequency_scores' collection.
    The retrieved documents are converted to a list and returned. Finally, the MongoDB client connection is closed.

    Returns:
        list: A list of documents retrieved from the 'frequency_scores' collection in the 'frequency_database'.
    """
    client = MongoClient("mongodb://127.0.0.1:27017/")
    db = client['frequency_database']
    collection = db['frequency_scores']
    documents = collection.find()
    data = list(documents)
    client.close()

    return data

def update_idf_for_all_filenames(idf):
    """
    Update the 'idf' field for all documents in the 'frequency_scores' collection of the 'frequency_database'.

    This function connects to a MongoDB instance, accesses the 'frequency_scores' collection in the 
    'frequency_database', and updates the 'idf' field for all documents with the provided 'idf' value.

    Args:
        idf (float): The IDF value to set for all documents in the collection.

    """
    client = MongoClient("mongodb://127.0.0.1:27017/")
    db = client['frequency_database']
    collection = db['frequency_scores']
    collection.update_many({}, {'$set': {'idf': idf}})
    print(f"IDF updated for all filenames")
    
    client.close()


def batch_job(cloud_tag_folder):
    """
    Performs a batch job to calculate TF-IDF values and generate tag clouds.

    This function reads all text data from a MongoDB database, calculates the Inverse Document Frequency (IDF)
    for all documents, and updates the IDF values in the database. Subsequently, it computes the TF-IDF values
    for each document and generates corresponding tag clouds. Finally, it creates a global tag cloud based
    on the TF values of all documents.

    Args:
    cloud_tag_folder (str): The path to the folder where the generated tag clouds should be saved.
    """
    
    data = read_all_data_from_mongo()
    texts = [d['text'] for d in data]
    filenames = [d['filename'] for d in data]
    tfs = [d['tf'] for d in data]
    idf = calculate_inverse_document_frequency_spark(texts)
    update_idf_for_all_filenames(idf)
    print("Document frequency calculation completed successfully")

    # for each file calculate tf-idf
    for filename, tf in zip(filenames, tfs):
        tf_idf = {word: tf[word] * idf[word] for word in tf if word in idf}
        filename = filename.replace('.txt', '')
        
        # generate tag clouds
        speed_layer.generate_tag_cloud(tf_idf, filename, is_batch=True)

    # Calculate global TF sum
    global_tf = Counter()
    for tf in tfs:
        global_tf += Counter(tf)

    # Calculate TF-IDF for global TF
    global_tf_idf = {word: global_tf[word] * idf[word] for word in global_tf if word in idf}

    print(cloud_tag_folder)

    speed_layer.generate_tag_cloud(global_tf_idf, "batch", is_batch=True)
    print("batch Tag cloud generated successfully")