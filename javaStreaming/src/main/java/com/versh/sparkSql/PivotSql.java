package com.versh.sparkSql;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class PivotSql {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkSession session = SparkSession.builder()
				.appName("SqlReadCsv") 
				.master("local[*]")
				.getOrCreate();

		Logger.getLogger("org.apache").setLevel(Level.ERROR);

		Dataset<Row> dataset = session.read().option("header", true).csv("src/main/resources/biglog.txt");

		//		Dataset<Row> results = spark.sql
		//		  ("select level, date_format(datetime,'MMMM') as month, count(1) as total " + 
		//		   "from logging_table group by level, month order by cast(first(date_format(datetime,'M')) as int), level");			

		dataset = dataset.select(col("level"),
				date_format(col("datetime"), "MMMM").alias("month"), 
				date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) );


		Object[] months = new Object[] { "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
		List<Object> columns = Arrays.asList(months);

		dataset = dataset.groupBy("level").pivot("month", columns).count();

		dataset.show(100);

		session.close();

	}

}
