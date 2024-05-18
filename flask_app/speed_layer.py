
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
    Process the input text by tokenizing it, removing stopwords, and non-alphabetic tokens.

    Args:
        text: The input text to be processed.

    Returns:
        The processed text after tokenization, stopword removal, and filtering of non-alphabetic tokens.
    """
    tokens = word_tokenize(text.lower())
    # Remove stopwords and non-alphabetic tokens
    filtered_tokens = [token for token in tokens if token.isalpha() and token not in stop_words]
    processed_text = ' '.join(filtered_tokens)

    return processed_text

def calculate_term_frequency_spark(spark_session, text):
    """
    Calculates the term frequency (TF) for the given text using Apache Spark.

    Args:
        spark_session: The SparkSession object.
        text: The input text for which the term frequency needs to be calculated.

    Returns:
        A dictionary containing the term frequency of each word in the text.
    """
    df = spark_session.createDataFrame([(text,)], ["text"])
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    wordsData = tokenizer.transform(df)
    cv = CountVectorizer(inputCol="words", outputCol="features")
    model = cv.fit(wordsData)
    result = model.transform(wordsData)
    vocabulary = model.vocabulary
    tf_dict = {vocabulary[i]: count for i, count in zip(result.select("features").rdd.flatMap(lambda x: x).collect()[0].indices, result.select("features").rdd.flatMap(lambda x: x).collect()[0].values)}

    return tf_dict


def init_inv_doc_freq(word_scores):
    # Initialize the inverse document frequency with a value of 1 for all words
    inv_doc_freq = {word: 1 for word in word_scores.keys()}
    return inv_doc_freq

def save_word_tf_idfs_to_mongo(filename, text, tf, idf):
    """
    Saves the word Term Frequency (TF) and Inverse Document Frequency (IDF) scores to a MongoDB database.

    Args:
        filename: The name of the file.
        text: The text content of the file.
        tf: A dictionary containing the Term Frequency (TF) scores of words.
        idf: A dictionary containing the Inverse Document Frequency (IDF) scores of words.
    """
    client = MongoClient("mongodb://127.0.0.1:27017/")
    db = client['frequency_database']
    collection = db['frequency_scores']
    word_scores_dict = {'filename': filename, 'text': text, 'tf': tf, 'idf': idf}
    collection.update_one({'filename': filename}, {'$set': word_scores_dict}, upsert=True)
    print(f"Word scores saved to MongoDB for file: {filename}")
    client.close()

def clear_database():
    client = MongoClient('localhost', 27017)
    client.drop_database('frequency_database')
    print("Database cleared successfully")
    client.close()

def generate_tag_cloud(tf_idf, file_path, is_batch=False):
    """
    Generate a tag cloud image based on TF-IDF scores and save it.

    Args:
        tf_idf: A dictionary containing word frequencies as keys and their corresponding TF-IDF scores as values.
        file_path: The path of the file from which the TF-IDF scores were generated.
        is_batch: Indicates if the function is called in batch mode. Defaults to False.

    Returns:
        The file path where the tag cloud image is saved.
    """
    tag_cloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(tf_idf)
    if not is_batch:
        filename = os.path.splitext(os.path.basename(file_path))[0]
        file_path = os.path.dirname(file_path)
        file_path = os.path.join(file_path, '..', 'tag_clouds')
        tag_cloud_path = os.path.join(file_path, f"{filename}_tag_cloud.png")
    else:
        tag_cloud_path = 'tag_clouds/' + file_path + '_tag_cloud.png'
    tag_cloud.to_file(tag_cloud_path)
    print(f"Tag cloud generated for file: {file_path}")
    return tag_cloud_path


def process_files_with_spark(filepaths):
    """
    Processes text files using Apache Spark to calculate TF-IDF scores and generates tag clouds.

    Args:
        filepaths: A list of filepaths or a single filepath.
    """
    if not isinstance(filepaths, list):
        filepaths = [filepaths]
    for filepath in filepaths:
        assert os.path.exists(filepath), f"Path {filepath} does not exist"
    
    # Initialize a SparkSession
    sc = SparkContext("local", "spark_app")
    spark = SparkSession(sc)
    for file_path in filepaths:
        filename = os.path.basename(file_path)
        with open(file_path, 'r') as file:
            text = file.read()
        text = process_text(text)
        tf = calculate_term_frequency_spark(spark, text)
        idf = init_inv_doc_freq(tf)

        # Save the word scores to MongoDB
        save_word_tf_idfs_to_mongo(filename, text, tf, idf)

        print(f"Calculating TF-IDF for file: {filename}")
        
        tf_idf = {word: tf[word] * idf[word] for word in tf.keys()}

        # generate tag cloud
        generate_tag_cloud(tf_idf, file_path)
        print(f"Processing completed for file: {filename}")

    sc.stop()
    spark.stop()
