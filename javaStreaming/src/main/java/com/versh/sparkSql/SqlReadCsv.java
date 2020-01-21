package com.versh.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class SqlReadCsv {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession session = SparkSession.builder()
				.appName("SqlReadCsv") 
				.master("local[*]")
				.getOrCreate();

		Logger.getLogger("org.apache").setLevel(Level.ERROR);


		Dataset<Row> dataset = session.read().option("header", true).csv("src/main/resources/exams/students.csv");

		//dataset.show();

		//Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007 ");

		//Dataset<Row> modernArtResults = dataset.filter(x->x.getAs("subject").equals("Modern Art"));

		//Dataset<Row> modernArtResults = dataset.filter(col("subject").equalTo("Modern Art")
//		.and(col("year").geq(2007)));

		//modernArtResults.show();

		dataset.createOrReplaceTempView("my_students_table");
		
		Dataset<Row> results = session.sql("select distinct(year) from my_students_table order by year desc");

		results.show();	
		
		session.close();

	}

}
