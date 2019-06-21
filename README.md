# 2019-reddit-comments
General analysis over reddit comments from April 2012, and the influence of worlwide events on them, with Apache Spark. 

**Group 14**

-Gustavo Álvarez.

-Bastián Matamala.

-Tomás Leyton.


# Overview

The main goal of the project is to perform a exploratory data analysis over reddit coments, trying to answer the following questions:

1. Number of comments per each day of the month 
* Over all web-page and on specific subreddits
2. Number of comments per hour
* Over all web-page and on specific subreddits
3. The influence of worldwide events on reddit's  users behaviour, and by "worldwide events" we mean the release of the second season of Game of Thrones (we also tried with Avengers, but the movie release was on May).

# Data

Due to the high quantity of comments in Reddit, we worked with only one month of data, Choosing April 2012 because on this month the second season of Game of Thrones was released (every Sunday on HBO), and this was a huge event specially for the Reddit community with a more "geek" profile.

The dataset we used can be found on the following link https://files.pushshift.io/reddit/comments/, where the comments are stored as a JSON Object. With the original BZIP2 compression its size is 1,786,140,247 bytes and once descompressed 10,994,516,204 bytes.

The data contains, according to the uploader, 19044534 JSON Objects, each object having the following structure as show on Image 1


**Image 1**:
![Imgur](https://i.imgur.com/OvWB9rU.jpg)

# Methods

We use  SPARK to perform a set of map/reduce tasks as follows:

1. To map we use  ((subreddit_id, subreddit), 1) pairs to map the comments per subreddit.
2. Reduce process works counting the number of comments by each subreddit.

With this pipeline we can count the comments per subreddit, and then also get the global count. We can, then, filter the results for specifics topics, like r/gameofthrones subreddit. We can also use the hour and day as key, and obtain the counts per timeframe.

Counting task performed:
- Counting comments by subreddit per day (filtering only those with more than 1000 comments).
- Counting comments by hour across the month for each subreddit (filtering only those with more than 1250 comments).
- Counting the total amount of karma (+1/-1) over all comments by hour.
- Counting the total comments per day and hour across all subreddits.

As a last task, we tried to find the amount of common redditors (or users) for each pair of subreddits.
Sadly, as this was such a huge operation, we were forced to limit our search to only valuable redditors: those with comments with more than 100 Karma (score) in each subreddit.

We cached the RDDs we were going to use more than one time, but we did not persist. Why? Mostly because errors such Thread Exceptions or Null Pointers Exceptions that did not happened without it. So...

# Results

We had some troubles working with the JSON Object, as the standard library provided for the lab didn't include DataFrames nor a method to read from a JSON. That was fixed downloading the additional libraries, after we failed to parse the data to TSV with PIG (because the comments had line breaks).

We also had some troubles trying to use the file compressed in BZ2, the process failed in the large file this process. A possible explanation is that CBZip2InputStream, which the current version of Hadoop in the server uses to decompress files in this format, is not threadsafe and fails when combining with Spark. We solve this decompressing the file before running our Spark tasks.

Our runtimes where about 6 to 20 minutes, the last one when we searched for common valuable redditors though subreddits.

We found the day withs more comments is Tuesday, almost every week, and the day with significantly fewer comments is Sunday. 

The hour of the day identified with almost the double of comments than the hour with least, with are between 05:00 and 15:00 hours UTC±00:00

The timeframe with more comments is between 05:00 and 15:00 UTC±00:00, almost doubling the hour with least comments. If we take in consideration the fact that Reddit was used mostly in the US, the results are consistent with the typical sleeping hours of most populated areas in North America.

The most popular subreddits in base of the number of comments are shown on Image 2:

**Image 2**:
![Imgur](https://i.imgur.com/Y4ZgfLc.jpg)


About Game of Thrones, we found the most commented days are those around the date of release (before and after) as shown on Image 3, with peaks the day inmediately after their airing:

**Image 3**:
![Imgur](https://i.imgur.com/jw01BgY.jpg)

On the other hand, we found that the most commented hours are not significantly different that the rest of Reddit, as shown on Image 4, but with a peak in the hours after the airing the episode (consideering that this happens usually at 01:00 AM UTC±00:00)

**Image 4**:
![Imgur](https://i.imgur.com/XYTvRxm.jpg)

Finally, on Image 4 it is shown the number of valuable users (those with comments with more than 100 points of Karma) per pair of subreddits (just the top ten):

**Image 5**:
![Imgur](https://i.imgur.com/oPsbGyy.jpg)
# Conclusion
As it is shown, Reddit activity is focused on laboral days, meaning, at least in 2012, it was an important tool for procrastination. Maybe it was not blocked in workplaces networks, like is the usual for Facebook, Instagram or more common social networks.

In this case, a relevant event like the premiere of each episode of the second season of GoT is followed with peaks of activity in the related subreddits, as predicted. We also found, although we did not include charts, that the number of comments in /r/masseffect is decreasing as the month passes, which makes sense when we consider the third part of the game was launched in March 2012.

Also is curious the fact there's just a few redditors that have comments with more than 100 Karma in at least two subreddits, meaning that it wasn't quite easy to obtain upvotes at the time.

One of the main conclusions is that distributed systems are very helpful to process huge amounts of data is less time. Sadly, we could not do everything we wanted, mostly because external reasons such unexpected behaviours with the server, or delays related to technical aspects.
# Appendix

[Count by Hour](https://i.imgur.com/c0IcG7k.jpg)
[Count by day of the month](https://i.imgur.com/ihm24NP.jpg)
