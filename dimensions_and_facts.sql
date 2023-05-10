
MSCK REPAIR TABLE twitter_data.tweets;

SET hive.exec.dynamic.partition.mode=nonstrict;

-- the diminsions:

-- 1- user table :

CREATE  TABLE IF NOT EXISTS twitter_data.users_dim (
        author_id STRING,
        id STRING,
        name STRING,
        username STRING,
        followers_count INT,
        following_count INT,
        tweet_count INT,
        listed_count INT,
        verified STRING
    )
    PARTITIONED BY (year INT, month INT, day INT)
    STORED AS PARQUET
    LOCATION '/FileStore/twitter_data/users_dim';


 INSERT OVERWRITE TABLE twitter_data.users_dim
    PARTITION (year, month, day)
    SELECT author_id, id, name, username, followers_count, following_count, tweet_count, listed_count, verified, year(created_at) AS year, month(created_at) AS month, day(created_at) AS day
    FROM twitter_data.tweets ; 


-- 2-tweets table :

CREATE  TABLE IF NOT EXISTS twitter_data.tweets_dim (
    id STRING ,
    text STRING,
    author_id STRING,
    retweet_count INT,
    reply_count INT,
    like_count INT,
    quote_count INT,
    created_at TIMESTAMP
   
) PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/FileStore/twitter_data/tweets_dim';

INSERT OVERWRITE TABLE twitter_data.tweets_dim
PARTITION (year, month, day, hour)
SELECT id, text, author_id, retweet_count, reply_count, like_count, quote_count, created_at, year(created_at) as year, month(created_at) as month, day(created_at) as day, hour(created_at) as hour
FROM twitter_data.tweets;

 

-- 3- Author matrix table:

CREATE EXTERNAL TABLE IF NOT EXISTS twitter_data.author_matrix (
    author_id STRING,
    tweet_id STRING
) PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/FileStore/twitter_data/author_matrix';


INSERT OVERWRITE TABLE twitter_data.author_matrix
PARTITION (year, month, day, hour)
SELECT author_id, id as tweet_id, year(created_at) as year, month(created_at) as month, day(created_at) as day, hour(created_at) as hour
FROM twitter_data.tweets_dim;

-- 4- -- time_dim_raw dimension table

CREATE EXTERNAL TABLE IF NOT EXISTS twitter_data.time_dim (
    time_id INT,
    created_at TIMESTAMP
)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/FileStore/twitter_data/time_dim';

INSERT OVERWRITE TABLE twitter_data.time_dim
PARTITION (year, month, day, hour)
SELECT
ROW_NUMBER() OVER (ORDER BY created_at) AS time_id,
created_at, year, month,day,hour
FROM twitter_data.time_dim;

-- -- facts
-- -- 4- Tweets with count more than 50 table:

-- CREATE TABLE IF NOT EXISTS twitter_data.tweets_more_than_50 (
--     hour INT,
--     tweets_count INT,
--     PRIMARY KEY (hour)
-- ) PARTITIONED BY (year INT, month INT, day INT)
-- STORED AS PARQUET
-- LOCATION '/FileStore/twitter_data/tweets_more_than_50';

-- INSERT OVERWRITE TABLE twitter_data.tweets_more_than_50
-- PARTITION (year, month, day)
-- SELECT hour, COUNT(*) AS tweets_count, year(created_at) AS year, month(created_at) AS month, day(created_at) AS day
-- FROM twitter_data.tweets_dim
-- GROUP BY hour, year, month, day
-- HAVING COUNT(*) > 50;


-- 5- Tweet Matrix Table

CREATE TABLE IF NOT EXISTS twitter_data.tweet_matrix_raw (
    id String ,
    retweet_count INT,
    reply_count INT,
    like_count INT,
    quote_count INT
) PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/FileStore/twitter_data/tweet_matrix_raw';

INSERT OVERWRITE TABLE  twitter_data.tweet_matrix_raw
PARTITION (year, month, day, hour)
SELECT id, retweet_count, reply_count, like_count, quote_count, 
  year(created_at) as year, month(created_at) as month, day(created_at) as day, hour(created_at) as hour
FROM twitter_data.tweets;


-- -- 6- Tweets Count Table

CREATE TABLE IF NOT EXISTS twitter_data.tweets_count_raw (
    hour INT,
    tweets_count INT
) PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION '/FileStore/twitter_data/tweets_count_raw';

INSERT OVERWRITE TABLE twitter_data.tweets_count_raw
PARTITION (year, month, day)
SELECT 
  HOUR(CAST(t.created_at AS TIMESTAMP)) AS hour,
  COUNT(1) AS tweets_count,
  YEAR(CAST(t.created_at AS DATE)) AS year,
  MONTH(CAST(t.created_at AS DATE)) AS month,
  DAY(CAST(t.created_at AS DATE)) AS day
FROM twitter_data.tweets t
GROUP BY HOUR(CAST(t.created_at AS TIMESTAMP)), YEAR(CAST(t.created_at AS DATE)), MONTH(CAST(t.created_at AS DATE)), DAY(CAST(t.created_at AS DATE));


-- 7- Most Active Hour Table

CREATE TABLE IF NOT EXISTS twitter_data.most_active_hour_raw (
    hour INT,
    tweets_count INT
) PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION '/FileStore/twitter_data/most_active_hour_raw';

INSERT OVERWRITE TABLE twitter_data.most_active_hour_raw
PARTITION (year, month, day)
SELECT hour(created_at) AS hour,
COUNT(*) AS tweets_count,
YEAR(created_at) AS year,
MONTH(created_at) AS month,
DAY(created_at) AS day
FROM twitter_data.tweets
GROUP BY hour(created_at), YEAR(created_at), MONTH(created_at), DAY(created_at);

-- -- 8- Tweets with More than 50 Count Table

CREATE TABLE IF NOT EXISTS twitter_data.tweets_more_than_50_raw (
    hour INT,
    tweets_count INT
) PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
LOCATION '/FileStore/twitter_data/tweets_more_than_50_raw';

INSERT OVERWRITE TABLE twitter_data.tweets_more_than_50_raw
PARTITION (year, month, day)
SELECT hour(created_at) as hour, COUNT(*) as tweets_count, 
  year(created_at) as year, month(created_at) as month, day(created_at) as day
FROM twitter_data.tweets
GROUP BY year(created_at), month(created_at), day(created_at), hour(created_at)
HAVING COUNT(*) > 50;


-- -- 9- captures the total tweet count at the hourly level

CREATE TABLE IF NOT EXISTS twitter_data.tweet_count_hourly (
    year INT,
    month INT,
    day INT,
    hour INT,
    tweet_count INT
) STORED AS PARQUET
LOCATION '/FileStore/twitter_data/tweet_count_hourly';


INSERT OVERWRITE TABLE twitter_data.tweet_count_hourly
SELECT
    year(created_at) AS year,
    month(created_at) AS month,
    day(created_at) AS day,
    hour(created_at) AS hour,
    COUNT(*) AS tweet_count
FROM twitter_data.tweets
GROUP BY year(created_at), month(created_at), day(created_at), hour(created_at);

-- -- 10-sorted retweeted tweets:

-- CREATE TABLE IF NOT EXISTS twitter_data.top_10_retweeted_tweets_fact (
--     tweet_id BIGINT,
--     text STRING,
--     retweet_count INT
-- ) STORED AS PARQUET
-- LOCATION '/FileStore/twitter_data/top_10_retweeted_tweets_fact';

-- INSERT INTO twitter_data.top_10_retweeted_tweets_fact
-- SELECT id AS tweet_id, text, retweet_count
-- FROM twitter_raw_data.tweets_raw
-- ORDER BY retweet_count DESC
-- LIMIT 10;








-- -- ** to get them :

-- SELECT * FROM twitter_raw_data.tweets_more_than_50_raw WHERE tweets_count > 50;


-- -- **Get the total number of tweets in each hour:

-- SELECT year, month, day, hour, COUNT(*) AS tweet_count
-- FROM twitter_raw_data.tweets_raw
-- GROUP BY year, month, day, hour;


-- **Get the top 10 most retweeted tweets:
-- SELECT text, retweet_count
-- FROM twitter_raw_data.tweets_raw
-- ORDER BY retweet_count ;


-- **Get the total number of users in each hour:
-- SELECT year, month, day, hour, COUNT(*) AS user_count
-- FROM twitter_raw_data.users_raw
-- GROUP BY year, month, day, hour;


-- ** Get the top 10 most followed users:

-- SELECT name, username, followers_count
-- FROM twitter_raw_data.users_raw
-- ORDER BY followers_count DESC
-- LIMIT 10;

-- **Get the total number of tweets each author has posted:

-- SELECT author_id, COUNT(*) AS tweet_count
-- FROM twitter_raw_data.author_matrix_raw
-- GROUP BY author_id;

-- **Get the top 10 authors with the most retweeted tweets:

-- SELECT author_id, COUNT(*) AS retweet_count
-- FROM twitter_raw_data.author_matrix_raw
-- WHERE retweet_count > 0
-- GROUP BY author_id
-- ORDER BY retweet_count DESC
-- LIMIT 10;


-- **Get the total number of interactions for each tweet:

-- SELECT tweet_id, SUM(retweet_count + reply_count + like_count + quote_count) AS interaction_count
-- FROM twitter_raw_data.tweet_matrix_raw
-- GROUP BY tweet_id;


-- **Get the top 10 most liked tweets:
-- SELECT tweet_id, like_count
-- FROM twitter_raw_data.tweet_matrix_raw
-- ORDER BY like_count DESC
-- LIMIT 10;
