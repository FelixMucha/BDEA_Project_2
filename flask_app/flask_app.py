from flask import Flask, render_template, request, redirect, url_for
import os
from werkzeug.utils import secure_filename

app = Flask(__name__)

# Define the upload folder
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), '..', 'doc_uploads')

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Allowed file extensions
ALLOWED_EXTENSIONS = {'txt'}

# Function to check if a file has an allowed extension
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    # Check if files were submitted
    if 'files[]' not in request.files:
        return redirect(request.url)
    
    files = request.files.getlist('files[]')

    # Iterate over each uploaded file
    for file in files:
        # Check if the file is empty
        if file.filename == '':
            return redirect(request.url)

        # Check if the file has an allowed extension
        if file and allowed_file(file.filename):
            # Save the uploaded file to the upload folder
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            
            # Perform processing on each uploaded file here (e.g., generate Tag Cloud)

    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, port=5000)
