package org.mdp.spark.cli;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Comparator;


public class CountBySubreddit {
    /**
     * Main function
     */
    public static void main(String[] args) {
        System.setProperty("user.timezone", "UTC");
        System.setProperty("hadoop.home.dir", "C:/Program Files/Hadoop/");
        // In case there's an error in input arguments
        if(args.length != 8) {
            System.err.println("Usage arguments: inputPath commentsBySubreddit countBySubredditDay countBySubredditHour karmaPerHour totalCountDay totalCountHour similarSubreddits");
            System.exit(0);
        }
        // Run process
        new CountBySubreddit().run(args[0],args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
    }


    /**
     * Task body
     */
    public void run(String inputFilePath,
                    String commentsBySubreddit,
                    String countBySubredditDay,
                    String countBySubredditHour,
                    String karmaPerHour,
                    String totalCountDay,
                    String totalCountHour,
                    String similarSubreddits) {
        // Initialises a Spark Session with the name of the application and the (default) master settings.
        SparkSession session = SparkSession.builder().master("local[*]").appName(CountBySubreddit.class.getName()).getOrCreate();


        // Load the first RDD from the input location (a local file, HDFS file, etc.)
        Dataset<Row> input_json = session.read().json(inputFilePath);

        // The rows we're interested in
        JavaRDD<Row> inputRDD = input_json.select("subreddit_id", "subreddit", "author", "created_utc", "score").toJavaRDD();

        inputRDD.cache(); // cache

        // Pairs key-value with the format ((subreddit_id, subreddit), 1)
        JavaPairRDD<Tuple2<String, String>, Integer> by_subreddit = inputRDD.mapToPair(
                row -> new Tuple2<>(
                        new Tuple2<String, String>(row.get(0).toString(), row.get(1).toString()),
                        1)
        );


        // Pairs key-value with the format ((subreddit_id, subreddit, day_of_month), 1)
        JavaPairRDD<Tuple3<String, String, Integer>, Integer> by_subreddit_day = inputRDD.mapToPair(
                row -> new Tuple2<>(
                        new Tuple3<String, String, Integer>(row.get(0).toString(), row.get(1).toString(), (new Timestamp((Long.parseLong(row.get(3).toString())) * 1000)).getDate()),
                        1
                )
        );

        // Pairs key-value with the format ((subreddit_id, subreddit, hour_of_day), score)
        JavaPairRDD<Tuple3<String, String, Integer>, Tuple2<Integer, Integer>> by_subreddit_hour = inputRDD.mapToPair(
                row -> new Tuple2<>(
                        new Tuple3<String, String, Integer>(row.get(0).toString(), row.get(1).toString(), (new Timestamp((Long.parseLong(row.get(3).toString())) * 1000)).getHours()),
                        new Tuple2<Integer, Integer>(1, Integer.parseInt(row.get(4).toString()))
                )
        );

        by_subreddit_hour.cache();

        // Reduce process to count the number of comments by each subreddit
        JavaPairRDD<Tuple2<String, String>, Integer> count_by_subreddit = by_subreddit.reduceByKey(
                Integer::sum
        );

        // Reduce process to count the number of comments by each subreddit per each day of the month
        JavaPairRDD<Tuple3<String, String, Integer>, Integer> count_by_subreddit_day = by_subreddit_day.reduceByKey(
                Integer::sum
        ).filter(row -> row._2() >= 1000);

        // Reduce process to count the number of comments by each subreddit per each hour of the day during the entire month
        JavaPairRDD<Tuple3<String, String, Integer>, Integer> count_by_subreddit_hour = by_subreddit_hour.mapToPair(
                row -> new Tuple2<>(new Tuple3<String, String, Integer>(row._1()._1(), row._1()._2(), row._1()._3()), row._2()._1())
        ).reduceByKey(
                Integer::sum
        );

        // When should I upload my meme? Maps to pair (hour_of_day, score)
        JavaPairRDD<Integer, Integer> score_by_hour = by_subreddit_hour.mapToPair(
                row -> new Tuple2<>(row._1()._3(), row._2()._2())
                ).reduceByKey(Integer::sum).sortByKey(true);

        count_by_subreddit_day.cache();
        count_by_subreddit_hour.cache();

        // A new map in order to sort by the count of comments
        JavaPairRDD<Integer, String> sorted_results = count_by_subreddit.mapToPair(
                row -> new Tuple2<Integer, String>(row._2(), row._1()._2())
        ).sortByKey(false);

        // A new map in order to sort by the day of the month, and then by the count of comments,
        // filtering results with less than 1000 comments
        JavaPairRDD<Tuple2<Integer, Integer>, String> sorted_results_day = count_by_subreddit_day.mapToPair(
                row -> new Tuple2<>(new Tuple2<>(row._1()._3(), row._2()), row._1()._2())
        ).filter(row -> row._1()._2() > 1000).sortByKey(new TupleComparatorDayComments());

        // A new map in order to sort by hour of the day, and then by the count of comments
        // filtering results with less than 1250 comments
        JavaPairRDD<Tuple2<Integer, Integer>, String> sorted_result_hour = count_by_subreddit_hour.mapToPair(
                row -> new Tuple2<>(new Tuple2<>(row._1()._3(), row._2()), row._1()._2())
        ).filter(row -> row._1()._2() > 1250).sortByKey(new TupleComparatorDayComments());

        // Total comments by each day
        JavaPairRDD<Integer, Integer> total_count_day = count_by_subreddit_day.mapToPair(
                row -> new Tuple2<Integer, Integer>(row._1()._3(), row._2())
        ).reduceByKey(Integer::sum).sortByKey(true);

        // Total comments by each hour
        JavaPairRDD<Integer, Integer> total_count_hour = count_by_subreddit_hour.mapToPair(
                row -> new Tuple2<Integer, Integer>(row._1()._3(), row._2())
        ).reduceByKey(Integer::sum).sortByKey(true);

        // Pairs (author, subreddit), but authors with comments with karma > 100
        JavaPairRDD<String, String> redditors_by_subreddit = inputRDD.filter(row -> Integer.parseInt(row.get(4).toString()) > 100).mapToPair(
                row -> new Tuple2<String, String>(row.get(2).toString(), row.get(1).toString())
        );

        redditors_by_subreddit.cache(); // cache

        // Join in order to create (author, subreddit1, subreddit2), mapping all the possible
        // combinations of subreddit where the author has commented.
        JavaPairRDD<String, Tuple2<String, String>> join_subreddits = redditors_by_subreddit.join(redditors_by_subreddit);

        // Pairs ((subreddit1, subreddit2), author) -> ((subreddit1, subreddit2), 1)
        // -> ((subreddit1, subreddit2), count)
        JavaPairRDD<Tuple2<String, String>, Integer> filter_join = join_subreddits.mapToPair(
                row -> new Tuple2<>(
                        new Tuple2<>(
                                row._2()._1(), row._2()._2()),
                        row._1())
        ).filter(
                row -> row._1()._1().compareTo(row._1()._2()) > 0
        ).distinct().mapToPair(
                row -> new Tuple2<>(row._1(), 1)
        ).reduceByKey(Integer::sum).filter(
                row -> row._2() > 0);

        JavaPairRDD<Integer, Tuple2<String, String>> sorted_subreddits = filter_join.mapToPair(
                row -> new Tuple2<>(row._2(), row._1())
        ).sortByKey(false);

        // Save results to output paths
        sorted_results.saveAsTextFile(commentsBySubreddit);
        sorted_results_day.saveAsTextFile(countBySubredditDay);
        sorted_result_hour.saveAsTextFile(countBySubredditHour);
        score_by_hour.saveAsTextFile(karmaPerHour);
        total_count_day.saveAsTextFile(totalCountDay);
        total_count_hour.saveAsTextFile(totalCountHour);
        sorted_subreddits.saveAsTextFile(similarSubreddits);

    }
}

/**
 * Comparator to sort by day/hour and then by count of comments
 */
class TupleComparatorDayComments implements Comparator<Tuple2<Integer,Integer>>, Serializable {
    private static final long serialVersionUID = 1L;
    @Override
    public int compare(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) {
        if (v1._1().compareTo(v2._1()) == 0) {
            return -v1._2().compareTo(v2._2());
        }
        return  v1._1().compareTo(v2._1());
    }
}
