package com.versh.sparkStream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class ViewingFiguresStructuredVersion {

	public static void main(String[] args) throws InterruptedException, StreamingQueryException{
		// TODO Auto-generated method stub
		SparkSession session = SparkSession.builder()
				.appName("ViewingFiguresStructuredVersion") 
				.master("local[*]")
				.getOrCreate();

		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

		session.conf().set("spark.sql.shuffle.partitions", "10");

		Dataset<Row> df = session.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "testLogs")
				.load();

		// start some dataframe operations
		df.createOrReplaceTempView("viewing_figures");

		// key, value, timestamp
		Dataset<Row> results = 
				session.sql("select window, cast (value as string) as course_name, sum(5) as seconds_watched from viewing_figures group by window(timestamp,'2 minutes'),course_name");

		StreamingQuery query = results
				.writeStream()
				.format("console")
				.outputMode(OutputMode.Update())
				.option("truncate", false)
				.option("numRows", 50)
				.start();

		query.awaitTermination();

	}

}
