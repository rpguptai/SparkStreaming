/**
 * 
 */
package com.versh.main;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

/**
 * @author 
 *
 */
public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub


		SparkSession session = SparkSession.builder()
				.appName("SparkJavaExample") 
				.master("local[*]")
				.getOrCreate();

		Logger.getLogger("org.apache").setLevel(Level.ERROR);

		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Friday 7 September 1854");
		inputData.add("WARN: Saturday 8 September 1942");


		JavaSparkContext sc = new JavaSparkContext(session.sparkContext());

		sc.parallelize(inputData)
		.mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0] , 1L  ))
		.reduceByKey((value1, value2) -> value1 + value2)
		.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));


		sc.close();
		session.close();
	}

}
