from pyspark import SparkContext
import os

from pyspark.mllib.feature import HashingTF, IDF
from nltk.corpus import stopwords
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import os
import string
import nltk
nltk.download('stopwords')

"""
NOTE: To run this code you need to install java and spark on your machine:

https://www.oracle.com/de/java/technologies/downloads/#jdk22-windows
https://www.apache.org/dyn/closer.lua/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
https://github.com/steveloughran/winutils/tree/master

After installing java, spark and hadoop binary you need to set up the hadoop home environment variable to point to the winutils directory.
"""

os.environ['HADOOP_HOME'] = "C:\\Program Files\\hadoop-2.7.1"
os.environ['PATH'] = f'{os.environ["HADOOP_HOME"]}\\bin;{os.environ["PATH"]}'

# path to dir where files are uploaded
path = './doc_uploads'
# check if the path exists and assert if it does not
assert os.path.exists(path), f"Path {path} does not exist"

# get spark context
sc = SparkContext("local", "spark_app")

text_files = sc.wholeTextFiles(path)

tokenized_texts = text_files.map(lambda x: (x[0], x[1].lower().translate(str.maketrans('', '', string.punctuation)).split()))

# Entferne Stoppwörter
stop_words = set(stopwords.words('german'))
filtered_texts = tokenized_texts.map(lambda x: (x[0], [word for word in x[1] if word not in stop_words]))

# Berechne die Termfrequenz
hashingTF = HashingTF()
tf = hashingTF.transform(filtered_texts.map(lambda x: x[1]))

# Berechne die inverse Dokumentenfrequenz
idf = IDF().fit(tf)
tfidf = idf.transform(tf)

# Sammle die TF-IDF-Werte für jede Datei
tfidf_values = tfidf.collect()

# Erzeuge die Tag Cloud für jede Datei
for i, (file_path, tfidf_vector) in enumerate(tfidf_values):
    file_name = os.path.basename(file_path)
    top_words = tfidf_vector.toArray().tolist()
    # Wähle die Top-Wörter aus
    top_words_indices = sorted(range(len(top_words)), key=lambda i: top_words[i], reverse=True)[:10]
    top_words_dict = {hashingTF.indexOf(str(i)): tfidf_vector[i] for i in top_words_indices if tfidf_vector[i] > 0}
    # Erzeuge die Tag Cloud
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(top_words_dict)
    # Speichere die Tag Cloud als Bilddatei
    wordcloud.to_file(f'tag_cloud_{file_name}.png')
    # Zeige die Tag Cloud an (optional)
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.show()



sc.stop()
