package com.versh.main;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadFromFile {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkSession session = SparkSession.builder()
				.appName("SparkJavaExample") 
				.master("local[*]")
				.getOrCreate();

		Logger.getLogger("org.apache").setLevel(Level.ERROR);

		JavaRDD<Row> lines = session.read().text("src/main/resources/subtitles/input.txt").javaRDD();

		lines.flatMap(x -> Arrays.asList(x.toString().split(" ")).iterator())
		.collect().forEach(System.out::println);;

	}

}
