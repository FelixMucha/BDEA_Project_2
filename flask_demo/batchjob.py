from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from pymongo import MongoClient
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# Verbindung zur MongoDB-Datenbank
client = MongoClient('mongodb://admin:password@localhost:27017/')
db = client['file_database']
collection = db['upload_files']

# Apache Spark Session starten
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# Daten aus MongoDB laden
data = spark.createDataFrame(collection.find({}, {"_id": 0}))

# Word Count mit Spark durchführen
word_count = data.select(explode(split(data['content'], ' ')).alias('word')) \
    .groupBy('word') \
    .count()

# Berechnung der Dokumentenfrequenzen
document_frequencies = word_count.collect()

# Speichern der berechneten Dokumentenfrequenzen in einer .txt-Datei
output_file = 'document_frequencies.txt'
with open(output_file, 'w') as f:
    for row in document_frequencies:
        f.write(f"{row['word']}: {row['count']}\n")

# Dokumentenfrequenzen in MongoDB speichern
for row in document_frequencies:
    db.document_frequencies.insert_one({'word': row['word'], 'count': row['count']})

# Tag Clouds für alle Dokumente
word_cloud_text = ' '.join([row['word'] for row in document_frequencies])
wordcloud = WordCloud(width=800, height=400, background_color='white').generate(word_cloud_text)
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.show()

# Spark Session beenden
spark.stop()
