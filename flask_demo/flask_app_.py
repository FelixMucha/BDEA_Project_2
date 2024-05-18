from flask import Flask, render_template, request, redirect, url_for, jsonify,  send_from_directory
from werkzeug.utils import secure_filename
from wordcloud import WordCloud
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
from pyspark.ml.feature import HashingTF, IDF

import os

app = Flask(__name__)

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), '..', 'doc_uploads')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

STATIC_FOLDER = os.path.join(os.path.dirname(__file__), '..', 'static')
app.config['STATIC_FOLDER'] = STATIC_FOLDER

ALLOWED_EXTENSIONS = {'txt'}

# Allowed extension
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Set up NLTK stopwords
stop_words = set(stopwords.words('german'))

# MongoDB Clients
client = MongoClient('mongodb://admin:password@localhost:27017/')
db = client['file_database']
collection_docs = db['upload_files']
collection_tf = db['df_values']

# Spark
spark = SparkSession.builder \
    .appName("TF-IDF Calculation") \
    .getOrCreate()

# Set up NLTK stopwords
stop_words = set(stopwords.words('german'))

#generate TF-IDF scores
def generate_tag_cloud(text, filename):
    # Tokens
    tokens = word_tokenize(text.lower())
    filtered_tokens = [token for token in tokens if token.isalpha() and token not in stop_words]
    processed_text = ' '.join(filtered_tokens)

    # TF-IDF
    tfidf = TfidfVectorizer()
    tfidf_matrix = tfidf.fit_transform([processed_text])
    
    feature_names = tfidf.get_feature_names_out()
    tfidf_scores = tfidf_matrix.toarray()[0]
    
    # Tag Cloud
    word_scores = {feature_names[i]: tfidf_scores[i] for i in range(len(feature_names))}
    tag_cloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(word_scores)
    tag_cloud_path = os.path.join(app.config['STATIC_FOLDER'], f"{filename}_tag_cloud.png")
    tag_cloud.to_file(tag_cloud_path)
    print(f"Tag cloud generated for file: {filename}")
    
@app.route('/')
def index():
    files = os.listdir(app.config['UPLOAD_FOLDER'])
    tag_clouds = [file for file in os.listdir(app.config['STATIC_FOLDER']) if file.endswith('_tag_cloud.png')]
    return render_template('index.html', files=files, tag_clouds=tag_clouds)

@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

@app.route('/static/<filename>')
def static_file(filename):
    return send_from_directory(app.config['STATIC_FOLDER'], filename)

@app.route('/file_content/<filename>')
def file_content(filename):
    try:
        with open(os.path.join(app.config['UPLOAD_FOLDER'], filename), 'r') as f:
            content = f.read()
        return content
    except FileNotFoundError:
        return 'File not found', 404

@app.route('/upload', methods=['POST'])
def upload_file():
    files = request.files.getlist('files[]')
    filenames = []

    if 'files[]' not in request.files:
        return redirect(request.url)

    for file in files:
        if file.filename == '':
            return redirect(request.url)

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            filenames.append(filename)
            
            with open(file_path, 'r') as f:
                text = f.read()
            collection_docs.insert_one({'filename': filename, 'content': text})

            # Process TF-IDF using Spark
            data = spark.createDataFrame(collection_docs.find({}, {"_id": 0, "content": 1}))
            word_count = data.select(explode(split(data['content'], ' ')).alias('word')) \
                .groupBy('word') \
                .count()

            df = word_count.collect()
            for row in df:
                document_frequency = collection_docs.count_documents({'content': {'$regex': row['word']}})
                collection_tf.insert_one({'word': row['word'], 'tfidf': row['count'], 'document_frequency': document_frequency})

            generate_tag_cloud(text, filename)

    return jsonify({'message': f'{len(filenames)} files uploaded and processed successfully', 'filenames': filenames})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
