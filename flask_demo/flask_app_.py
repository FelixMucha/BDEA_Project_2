from flask import Flask, render_template, request, redirect, url_for, jsonify,  send_from_directory
from werkzeug.utils import secure_filename
from wordcloud import WordCloud
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from pymongo import MongoClient

import os

app = Flask(__name__)

# Define the upload folder
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), '..', 'doc_uploads')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Define the static folder for tag cloud images
STATIC_FOLDER = os.path.join(os.path.dirname(__file__), '..', 'static')
app.config['STATIC_FOLDER'] = STATIC_FOLDER

# Allowed file extensions
ALLOWED_EXTENSIONS = {'txt'}

# Set up NLTK stopwords
stop_words = set(stopwords.words('german'))

# Connect to MongoDB 
# mongo_uri = f"mongodb://{os.environ['MONGO_USERNAME']}:{os.environ['MONGO_PASSWORD']}@localhost:27017/"
# client = MongoClient(mongo_uri)

client = MongoClient('mongodb://admin:password@localhost:27017/')
db = client['file_database']
collection = db['upload_files']

# Function to check if a file has an allowed extension
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Function to process text and generate tag cloud
def generate_tag_cloud(text, filename):
    # Tokenize the text
    tokens = word_tokenize(text.lower())
    # Remove stopwords and non-alphabetic tokens
    filtered_tokens = [token for token in tokens if token.isalpha() and token not in stop_words]
    # Join the tokens back into a single string
    processed_text = ' '.join(filtered_tokens)
    # Create TF-IDF matrix
    tfidf = TfidfVectorizer()
    tfidf_matrix = tfidf.fit_transform([processed_text])
    # Extract feature names and TF-IDF scores
    feature_names = tfidf.get_feature_names_out()
    tfidf_scores = tfidf_matrix.toarray()[0]
    # Create a dictionary of words and their TF-IDF scores
    word_scores = {feature_names[i]: tfidf_scores[i] for i in range(len(feature_names))}
    # Generate tag cloud
    tag_cloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(word_scores)
    # Save the tag cloud image
    tag_cloud_path = os.path.join(app.config['STATIC_FOLDER'], f"{filename}_tag_cloud.png")
    tag_cloud.to_file(tag_cloud_path)
    print(f"Tag cloud generated for file: {filename}")

# return render_template('index.html')
@app.route('/')
def index():
    # Get list of files in the upload folder
    files = os.listdir(app.config['UPLOAD_FOLDER'])
    # Get list of tag clouds in the static folder
    tag_clouds = [file for file in os.listdir(app.config['STATIC_FOLDER']) if file.endswith('_tag_cloud.png')]
    return render_template('index.html', files=files, tag_clouds=tag_clouds)
    # return render_template('index.html')

# Route to serve static files (uploaded files and tag cloud images)
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

    # Check if files were submitted
    if 'files[]' not in request.files:
        return redirect(request.url)

    # Iterate over each uploaded file
    for file in files:
        # Check if the file is empty
        if file.filename == '':
            return redirect(request.url)

        # Check if the file has an allowed extension
        if file and allowed_file(file.filename):
            # Save the uploaded file to the upload folder
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            filenames.append(filename)
            
            # Insert file into MongoDB
            with open(file_path, 'r') as f:
                text = f.read()
            collection.insert_one({'filename': filename, 'content': text})

            # Perform processing on each uploaded file here (e.g., generate Tag Cloud)
            with open(file_path, 'r') as f:
                text = f.read()
            generate_tag_cloud(text, filename)
        
    return jsonify({'message': f'{len(filenames)} files uploaded and processed successfully', 'filenames': filenames})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
