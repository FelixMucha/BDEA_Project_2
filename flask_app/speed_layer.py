from pyspark import SparkContext
import os


# Function to process the uploaded files using Spark
def process_file_with_spark(filepath):
    # Initialize a SparkSession
    sc = SparkContext("local", "spark_app")
    # path to dir where files are uploaded
    assert os.path.exists(filepath), f"Path {filepath} does not exist"
    text_file = sc.textFile(filepath)
    
    print(" ------ Speed Layer ------ ")
    print("First 5 lines of the text file:")
    for line in text_file.collect()[:5]:
        print(line)
    print(" ------ End Speed Layer ------ ")
    
    sc.stop()
