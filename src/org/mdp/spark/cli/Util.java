package org.mdp.spark.cli;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class Util {
	public static <E> void dump(JavaRDD<E> rdd){
		List<E> results = rdd.collect();
		for(E result: results){
			System.out.println(result);
		}
	}
	
	public static <E,F> void dump(JavaPairRDD<E,F> rdd){
		List<Tuple2<E,F>> results = rdd.collect();
		for(Tuple2<E,F> result: results){
			System.out.println(result._1 +" " +result._2);
		}
	}
}
