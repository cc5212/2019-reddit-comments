package org.mdp.spark.cli;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Comparator;


public class CountBySubreddit {
    /**
     * Main function
     */
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:/Program Files/Hadoop/");
        // In case there's an error in input arguments
        if(args.length != 5) {
            System.err.println("Usage arguments: inputPath commentsBySubreddit countBySubredditDay countBySubredditHour karmaPerHour");
            System.exit(0);
        }
        // Run process
        new CountBySubreddit().run(args[0],args[1], args[2], args[3], args[4]);
    }


    /**
     * Task body
     */
    public void run(String inputFilePath, String commentsBySubreddit, String countBySubredditDay, String countBySubredditHour, String karmaPerHour) {

        // Initialises a Spark Session with the name of the application and the (default) master settings.
        SparkSession session = SparkSession.builder().master("local[*]").appName("jsonReader").getOrCreate();


        // Load the first RDD from the input location (a local file, HDFS file, etc.)
        Dataset<Row> input_json = session.read().json(inputFilePath);

        // The rows we're interested in
        JavaRDD<Row> inputRDD = input_json.select("subreddit_id", "subreddit", "author", "body", "created_utc", "score").toJavaRDD();

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
                        new Tuple3<String, String, Integer>(row.get(0).toString(), row.get(1).toString(), (new Timestamp((Long.parseLong(row.get(4).toString())) * 1000)).getDate()),
                        1
                )
        );

        // Pairs key-value with the format ((subreddit_id, subreddit, hour_of_day), 1)
        JavaPairRDD<Tuple3<String, String, Integer>, Tuple2<Integer, Integer>> by_subreddit_hour = inputRDD.mapToPair(
                row -> new Tuple2<>(
                        new Tuple3<String, String, Integer>(row.get(0).toString(), row.get(1).toString(), (new Timestamp((Long.parseLong(row.get(4).toString())) * 1000)).getHours()),
                        new Tuple2<Integer, Integer>(1, Integer.parseInt(row.get(5).toString()))
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
        ).filter(row -> row._2() >= 1000); // Filter results with less than 1000 comments

        // Reduce process to count the number of comments by each subreddit per each hour of the day during the entire month
        JavaPairRDD<Tuple3<String, String, Integer>, Integer> count_by_subreddit_hour = by_subreddit_hour.mapToPair(
                row -> new Tuple2<>(new Tuple3<String, String, Integer>(row._1()._1(), row._1()._2(), row._1()._3()), row._2()._1())
        ).reduceByKey(
                Integer::sum
        ).filter(row -> row._2() >= 10000); // Filter results with less than 1000 comments

        // When should I upload my meme? Maps to pair (hour_of_day, score)
        JavaPairRDD<Integer, Integer> score_by_hour = by_subreddit_hour.mapToPair(
                row -> new Tuple2<>(row._1()._3(), row._2()._2())
                ).reduceByKey(Integer::sum).sortByKey(true);

        count_by_subreddit_day.cache();
        count_by_subreddit_hour.cache();

        // A new map in order to sort by the count of comments
        JavaPairRDD<Integer, String> sorted_results = count_by_subreddit.mapToPair(
                row -> new Tuple2<Integer, String>(row._2(), row._1()._2())
        );

        // A new map in order to sort by the day of the month, and then by the count of comments
        JavaPairRDD<Tuple2<Integer, Integer>, String> sorted_results_day = count_by_subreddit_day.mapToPair(
                row -> new Tuple2<>(new Tuple2<>(row._1()._3(), row._2()), row._1()._2())
        ).sortByKey(new TupleComparatorDayComments());

        // A new map in order to sort by hour of the day, and then by the count of comments
        JavaPairRDD<Tuple2<Integer, Integer>, String> sorted_result_hour = count_by_subreddit_hour.mapToPair(
                row -> new Tuple2<>(new Tuple2<>(row._1()._3(), row._2()), row._1()._2())
        ).sortByKey(new TupleComparatorDayComments());

        sorted_results.saveAsTextFile(commentsBySubreddit);
        sorted_results_day.saveAsTextFile(countBySubredditDay);
        sorted_result_hour.saveAsTextFile(countBySubredditHour);
        score_by_hour.saveAsTextFile(karmaPerHour);

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
