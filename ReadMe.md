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

## 📊 Input Data Format

```csv
user_id,movie_id,timestamp,duration_watched
U1,M1,2025-04-01,2500
U2,M2,2025-04-01,160
...
