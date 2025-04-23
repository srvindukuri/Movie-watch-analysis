# 🎬 Movie Watch Behavior Analysis using PySpark

This project analyzes user watch logs from a streaming service using **Apache Spark**. It simulates real-world use cases like binge-watching detection, short watch behavior, and classifying viewer types (Light, Moderate, Heavy).

## 📁 Project Structure

- `Project1.py`: Main PySpark code for analysis
- `data/watch_logs.csv`: Input CSV file with watch logs
- `output/watchlogs_classified.csv`: Output CSV with viewer classification
- `README.md`: This documentation

## 🛠️ Technologies

- Apache Spark
- PySpark (DataFrames, SQL, UDFs)
- Python 3.x

🚀 Key Features
1. 🔍 Short Watch Analysis
Filters users who watched less than 200 seconds of a movie

Identifies top 5 movies with most short views

2. 🍿 Binge Watch Detection
Identifies users who watched more than 3 movies in a day

3. 🧠 Viewer Type Classification (UDF)
Light Viewer: < 30 mins

Moderate Viewer: 30–90 mins

Heavy Viewer: > 90 mins

4. 📝 Output
Adds a viewer_type_column to each row

Saves the result as CSV for reporting

✅ Sample Output (Final DataFrame)

user_id	movie_id	timestamp	duration_watched	viewer_type_column
U1	M1	2025-04-01	2500	Moderate Viewer
U2	M2	2025-04-01	160	Light Viewer
