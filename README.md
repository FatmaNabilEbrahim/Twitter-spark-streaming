Sure, here's a combined README file that incorporates the information from both files:

# Twitter Data Analysis using Apache Spark Streaming

This project utilizes Apache Spark and Spark Streaming to process real-time data streams from the Twitter API, extracting meaningful insights and analyzing large volumes of tweets in real-time.

## Requirements

The following tools and libraries are required to run the project:

- Python 3.6+
- Apache Spark 2.4+
- Hadoop 2.7+
- PySpark 2.4+ (for Spark Streaming)
- Hive 2.3+ (for Hive Metastore)
- Tweepy library for Python

## Components

The project includes the following components:

### Twitter Listener (twitter_listener.py)

A Python script that extracts data from the Twitter API every 5 minutes and sends it as a JSON file to a socket stream connected to Spark Streaming.

### Spark Streaming Script (spark_streaming.py)

A Python script that receives the JSON files from the socket stream and writes them as Parquet files on HDFS, partitioned by year, month, day, and hour.

### Hive Dimensions and facts  Script (tables.sql)

A SQL script that creates three tables (twitter_landing_table, users_raw, tweets_raw) and implements a Slowly Changing Dimension (SCD) in the users_raw table to merge new data with existing data based on the user_id column.


A Python script that extracts data from the dimensions tables using SparkSQL with Hive Metastore, generates a new attribute (Trust_Ratio_Perc) on the fly using SQL, extracts popular hashtags as a dimension, and writes the processed data as a table on HDFS.

## Usage

To run the project, follow these steps:

1. Clone the repository to your local machine.
2. Modify the `twitter_listener.py` script with your Twitter API credentials and run it to start extracting data from the Twitter API.
3. Modify the `spark_streaming.py` script with your HDFS path and run it to start processing the data streams.
4. Modify the `hive_script.sql` script with your database and table names and run it to create the required tables and implement the SCD.
5. Modify the `fact_processing.py` script with your HDFS paths and run it to generate the processed data table.

## Credits

This project was developed by [Fatma Nabil ] as part of the [Data Management] track .
