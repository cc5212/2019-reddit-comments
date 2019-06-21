# 2019-reddit-comments
General Analysis of reddit comment on April 2012 and influence of worlwide events, with Apache Spark. (Gustavo Álvarez, Bastián Matamala, Tomás Leyton. Group 14)


# Overview

The mail goal of the project is perform a exploratory data analysis on reddit coments , our main goal is answer the following questions:

1. Number of comments each day of the month 
* Over all web-page and on specific reddit topic
2. Number of comments at each hour of the day
* Over all web-page and on specific reddit topic
3. Influence of worldwide events on reddit  users behaviour (all web and topic-specific) like Game of thrones episode release

# Data

Due to the high quantity of record we worked with only a month of data, the month we choose is April of 2012 because on this month the first movie of avengers was release and also the second season of Game of Thrones, both worldwide events enough relevant to burst the web with user's opinions

The file we used can be find in the following link https://files.pushshift.io/reddit/comments/, and the comments are stored as a JSON Object. With the original BZIP2 compression its size is 1,786,140,247 bytes and once descompressed 10,994,516,204 bytes.

The data contains 19044534 JSON Objects, each object have the following structure as show on Image 1


**Image 1**:
![Imgur](https://i.imgur.com/OvWB9rU.jpg)

# Methods

We use  SPARK to perform a map reduce task as follows:

1. To map we use  ((subreddit_id, subreddit), value) pairs to map the coments.
2. Reduce process works counting the number of comments by each subreddit


With this pipeline we can count comments in almost every way we can, for example to count the coments on specific Game of Thrones reddit on a specific day or hour (also both).

Counting task performed:
- Counting comments by subreddit subreddit everyday (more than 1000 comments).
- Counting comments by hour across the month for each subreddit.
- Counting karma (+1/-1) by hour.
- Counting comments by day.

As a last task, we tried to find the amount of common redditors (or users) for each pair of subreddits.
Sadly, as this was such a huge operation, we were forced to limit our search to only valuable redditors: those with comments with more than 100 Karma (score) in each subreddit.

# Results

We have some troubles trying to use the file in bz2 format, the process failed due anomaly HADOOP-10614, described as CBZip2InputStream is not threadsafe. We solve this 

We found the most commented day is Tuesday every week, and the day with significantly fewer comments is Sunday. 

The period  of the day  identified with almost double of comments than the hour with least, with are between 05:00 and 15:00 hours UTC±00:00

The five most popular sub reddit in base of the number of comments are the following:
1. AskReddit
2. funny
3. pics
4. gaming
5. atheism

For the last task we had memory issues, what we did was filtering the data in order to found the most relevant users

# Conclusion

# Appendix

