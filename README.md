# Big Data Engineering and Analytics Project

This repository contains the source code for an exercise in the field of Big Data Engineering and Analytics as part of the Master of Computer Science, SS2024.

## Project Goal
The goal of this project is to develop a small web application based on the Lambda Architecture for analyzing large document collections. This application will include functionalities such as generating Tag Clouds using the TF-IDF method, uploading text files via a web interface, storing them in the file system (typically HDFS in practice), and performing batch processing tasks.

![Alt-Text](readme_data/bdea_a2_diagram.jpg)

## Key Features
- **Upload of Text Files:** Users can upload text files through a web interface. Optionally, support for additional file types can be added.
- **Tag Cloud Generation:** Upon uploading a file, a Tag Cloud will be generated using the TF-IDF method. Stopwords (words with less than 4 characters) will be removed. Term frequency (TF) will be derived directly from the document.
- **Batch Job for Document Frequency (DF):** A batch job will calculate the Document Frequency (DF) of each word and store it in a database. If the batch hasn't run yet, the DF will be set to 1.
- **Selection and Display of Tag Clouds:** Users can select a file from a list of uploaded files and view its corresponding Tag Cloud in the browser.
- **WordCount Batch:** Users can manually trigger a WordCount batch process using Hadoop or Spark. This batch will determine the document frequencies of words and store them in the database. Additionally, it will recalculate the Tag Clouds for each document.
- **Global TF-Sum Tag Cloud:** A Tag Cloud based on the global TF sum through document frequency will be generated.

## Implementation Recommendation
For implementing the web functionalities, it is recommended to use Flask.

## Technologies Used
- Flask (for web functionalities)
- Spark (for batch processing)
- Mongo DB Database (for storing document frequencies and other relevant data)

## Instructions for Running the Project
1. Clone this repository to your local machine.
2. Install the required dependencies.
3. Run the Flask application.
4. Navigate to the web interface (http://localhost:5000) and start using the application.
5. End application by strg + c

## Contributors
- Karel Kouambeng Fosso (karel.kouambengfosso@stud.hs-mannheim.de)
- Felix Mucha (felixjanmichael.mucha@stud.hs-mannheim.de)

## License
This project is licensed under the [GNU General Public License].
