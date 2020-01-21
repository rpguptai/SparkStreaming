package com.versh.sparkStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class LogStreamAnalysis {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub

		SparkSession session = SparkSession.builder()
				.appName("LogStreamAnalysis") 
				.master("local[*]")
				.getOrCreate();

		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		JavaSparkContext ctx = JavaSparkContext.fromSparkContext(session.sparkContext());
		JavaStreamingContext sc = new JavaStreamingContext(ctx, Durations.seconds(20)); //batch size
		
		JavaReceiverInputDStream<String> inputData = sc.socketTextStream("localhost", 8989);

		JavaDStream<String> results = inputData.map(item -> item);
		JavaPairDStream<String, Long> pairDStream = results.mapToPair(rawLogMessage -> new Tuple2<> (rawLogMessage.split(",")[0],1L));
		pairDStream = pairDStream.reduceByKeyAndWindow((x, y) -> x + y, Durations.minutes(2)); // window size

		// ========================================== window
		// ========   =========   ======== ========== batch
		pairDStream.print();

		sc.start();
		sc.awaitTermination();
		sc.close();

	}

}
