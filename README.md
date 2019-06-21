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

We have some troubles trying to use the file in bz2 format, the process failed due anomaly HADOOP-10614, described as CBZip2InputStream is not threadsafe. We solve this using new packages.

Our runtimes where about 6 to 20 minutes.

We found the most commented day is Tuesday every week, and the day with significantly fewer comments is Sunday. 

The period  of the day  identified with almost double of comments than the hour with least, with are between 05:00 and 15:00 hours UTC±00:00

The five most popular sub reddit in base of the number of comments are shown on Image 2:

**Image 2**:
![Imgur](https://i.imgur.com/Y4ZgfLc.jpg)


About Game of Thrones, we found the most commented days are those arround the date of release (before and after) as shown on Image 3:

**Image 3**:
![Imgur](https://i.imgur.com/jw01BgY.jpg)

On the other hand, we found that the most commented hours are not significantly different that the rest of Reddit, as shown on Image 4.

**Image 4**:
![Imgur](https://i.imgur.com/XYTvRxm.jpg)

Finally, on Image 4 its shown how many users with more than 100 points are in both subreddit. We filter it due to memory issues

**Image 5**:
![Imgur](https://i.imgur.com/oPsbGyy.jpg)
# Conclusion

# Appendix


[Count by Hour](https://i.imgur.com/c0IcG7k.jpg)
[Coun by day of the month](https://i.imgur.com/ihm24NP.jpg)
