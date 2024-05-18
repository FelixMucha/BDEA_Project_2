from flask import Flask, render_template, request, redirect, url_for, flash
import os
from werkzeug.utils import secure_filename
import speed_layer as speed_layer
import batch_layer as batch_layer
import time
from flask import send_from_directory

# Setuo the environment variables for Hadoop and Python
path_to_python = "C:\\Users\\felix\\AppData\\Local\\Programs\\Python\\Python310\\python.exe"
path_to_hadoop = "C:\\Program Files\\hadoop-3.3.5"

assert os.path.exists(path_to_hadoop), f"Path {path_to_hadoop} does not exist"
assert os.path.exists(path_to_python), f"Path {path_to_python} does not exist"

os.environ['PYSPARK_PYTHON'] = path_to_python
os.environ['HADOOP_HOME'] = path_to_hadoop
os.environ['PATH'] = f'{os.environ["HADOOP_HOME"]}\\bin;{os.environ["PATH"]}'



app = Flask(__name__)

# Define the upload folder
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), '..', 'doc_uploads')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Define the tag cload folder
TAG_CLOUD_FOLDER = os.path.join(os.path.dirname(__file__), '..', 'tag_clouds')
app.config['TAG_CLOUD_FOLDER'] = TAG_CLOUD_FOLDER

@app.route('/tag_clouds/<filename>')
def tag_cloud(filename):
    filename = filename.replace('.txt', '')
    return send_from_directory(app.config['TAG_CLOUD_FOLDER'], filename)


# Allowed file extensions
ALLOWED_EXTENSIONS = {'txt'}

# Function to check if a file has an allowed extension
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    # Get a list of files in the upload folder
    files = os.listdir(app.config['UPLOAD_FOLDER'])
    # if empty list, return a message
    if len(files) == 0:
        files = ['no files uploaded yet...']
    # Get list of tag clouds in the static folder
    tag_clouds = [file for file in os.listdir(app.config['TAG_CLOUD_FOLDER']) if file.endswith('_tag_cloud.png')]
    # Get the message from the query string
    message = request.args.get('message', default=None)
    # time=time for testing purposes 
    return render_template('index.html', time=time, files=files, tag_clouds=tag_clouds, message=message)

@app.route('/upload', methods=['POST'])
def upload_file():
    # Get the uploaded files
    files = request.files.getlist('files[]')
    # Check if files were submitted
    if not files or files[0].filename == '':
        warning_message = 'No file selected for uploading. Click first on "Choose Files" and then on "Upload Files".'
        return redirect(url_for('index', message=warning_message))
    
    # Save all the uploaded files
    file_paths = []
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
            file_paths.append(file_path)
    # print successfully uploaded files
    print(f"Files uploaded successfully: {file_paths}")
    # Perform processing on the uploaded files
    speed_layer.process_files_with_spark(file_paths)

    return redirect(url_for('index'))


@app.route('/process_file/<filename>', methods=['GET'])
def process_file(filename):
    # Construct the full file path
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    # Check if the file exists
    if not os.path.exists(file_path):
        return "File not found", 404

    # Process the file with Spark
    speed_layer.process_file_with_spark(file_path)

    # Redirect back to the index page
    return redirect(url_for('index'))


@app.route('/wordcount', methods=['POST'])
def wordcount():
    # Perform word count operation here
    # For example, you can call a function from spark_processing module
    batch_layer.word_count(app.config['TAG_CLOUD_FOLDER'])

    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, port=5000)
