
from pyspark import SparkContext
import os
from wordcloud import WordCloud
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from pymongo import MongoClient
from pyspark.ml.feature import Tokenizer, CountVectorizer
from pyspark.sql import SparkSession
import nltk

nltk.download('punkt')
# Set up NLTK stopwords
stop_words = set(stopwords.words('german'))


def process_text(text):
    """
    Process the given text by tokenizing it, removing stopwords and non-alphabetic tokens,
    and joining the remaining tokens back into a single string.

    Args:
        text (str): The text to be processed.

    Returns:
        str: The processed text.
    """
    # Tokenize the text
    tokens = word_tokenize(text.lower())
    # Remove stopwords and non-alphabetic tokens
    filtered_tokens = [token for token in tokens if token.isalpha() and token not in stop_words]
    # Join the tokens back into a single string
    processed_text = ' '.join(filtered_tokens)

    return processed_text

# Function to process the uploaded files using Spark
def calculate_term_frequency_spark(sparc_session, text):
    # Create a DataFrame with the text
    df = sparc_session.createDataFrame([(text,)], ["text"])
    # Tokenize the text
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    wordsData = tokenizer.transform(df)
    # Calculate term frequency
    cv = CountVectorizer(inputCol="words", outputCol="features")
    model = cv.fit(wordsData)
    result = model.transform(wordsData)
    # Get the vocabulary (list of words)
    vocabulary = model.vocabulary
    # Convert the features to a dictionary
    tf_dict = {vocabulary[i]: count for i, count in zip(result.select("features").rdd.flatMap(lambda x: x).collect()[0].indices, result.select("features").rdd.flatMap(lambda x: x).collect()[0].values)}

    return tf_dict


def init_inv_doc_freq(word_scores):
    # Initialize the inverse document frequency with a value of 1 for all words
    inv_doc_freq = {word: 1 for word in word_scores.keys()}
    return inv_doc_freq

def save_word_tf_idfs_to_mongo(filename, text, tf, idf):
    client = MongoClient("mongodb://127.0.0.1:27017/")
    db = client['frequency_database']
    collection = db['frequency_scores']
    # Create a dictionary to store the word scores
    word_scores_dict = {'filename': filename, 'text': text, 'tf': tf, 'idf': idf}
    # Update the document with the same filename, or insert a new document if it doesn't exist
    collection.update_one({'filename': filename}, {'$set': word_scores_dict}, upsert=True)
    print(f"Word scores saved to MongoDB for file: {filename}")
    # Close the MongoDB client
    client.close()
    """
    # Insert the word scores into the MongoDB collection
    collection.insert_one(word_scores_dict)
    print(f"Word scores saved to MongoDB for file: {filename}")
    # Close the MongoDB client
    client.close()
    """

def clear_database():
    # Create a MongoClient to the running mongod instance
    client = MongoClient('localhost', 27017)

    # Drop the database
    client.drop_database('frequency_database')

    print("Database cleared successfully")

    # Close the MongoDB client
    client.close()

# Function to process generate tag cloud
def generate_tag_cloud(tf_idf, file_path, is_batch=False):
    # Generate tag cloud
    tag_cloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(tf_idf)
    if not is_batch:
        # get file name from file path
        filename = os.path.splitext(os.path.basename(file_path))[0]
        # get path without file name
        file_path = os.path.dirname(file_path)
        # go to parent dir and than to tag_clouds
        file_path = os.path.join(file_path, '..', 'tag_clouds')
        # Save the tag cloud image
        tag_cloud_path = os.path.join(file_path, f"{filename}_tag_cloud.png")
    else:
        tag_cloud_path = 'tag_clouds/' + file_path + '_tag_cloud.png'
    tag_cloud.to_file(tag_cloud_path)
    print(f"Tag cloud generated for file: {file_path}")
    return tag_cloud_path


# Function to process the uploaded files using Spark
def process_files_with_spark(filepaths):
    # check if filepaths is a list
    if not isinstance(filepaths, list):
        filepaths = [filepaths]
    # check if filepaths are valid
    for filepath in filepaths:
        assert os.path.exists(filepath), f"Path {filepath} does not exist"

    # drop the database
    #clear_database()
    
    # Initialize a SparkSession
    sc = SparkContext("local", "spark_app")
    spark = SparkSession(sc)
    # Read all files at once
    #text_files_rdd = sc.wholeTextFiles(','.join(filepaths))
    # TODO: parallelize and not loop through all files
    # loop through all files
    for file_path in filepaths:
        # Get the filename from the file path
        filename = os.path.basename(file_path)
        # Read the file
        with open(file_path, 'r') as file:
            text = file.read()
        #file = sc.textFile(file)
        text = process_text(text)
        # Calculate term frequency
        tf = calculate_term_frequency_spark(spark, text)
        # Calculate inverse document frequency
        idf = init_inv_doc_freq(tf)
        # Save the word scores to MongoDB
        save_word_tf_idfs_to_mongo(filename, text, tf, idf)
        # Calculate TF-IDF
        print(f"Calculating TF-IDF for file: {filename}")
        # print keys of tf and idf
        tf_idf = {word: tf[word] * idf[word] for word in tf.keys()}
        # generate tag cloud
        generate_tag_cloud(tf_idf, file_path)
        print(f"Processing completed for file: {filename}")

    # Stop the SparkContext
    sc.stop()
    spark.stop()
