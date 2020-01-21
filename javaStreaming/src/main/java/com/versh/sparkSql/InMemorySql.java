package com.versh.sparkSql;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class InMemorySql {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkSession session = SparkSession.builder()
				.appName("SqlReadCsv") 
				.master("local[*]")
				.getOrCreate();

		Logger.getLogger("org.apache").setLevel(Level.ERROR);

		List<Row> inMemory = new ArrayList<Row>();

		inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
		inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
		inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
		inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
		inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

		StructField[] fields = new StructField[] {
				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
				new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
		};

		StructType schema = new StructType(fields);
		Dataset<Row> dataset = session.createDataFrame(inMemory, schema);

		dataset.createOrReplaceTempView("logging_table");

		Dataset<Row> results = session.sql("select level, date_format(datetime,'MMMM') as month, count(1) as total from logging_table group by level, month");

		results.show();

		Dataset<Row> bigdataset = session.read().option("header", true).csv("src/main/resources/biglog.txt");

		bigdataset.createOrReplaceTempView("big_logging_table");

		Dataset<Row> bigResults = session.sql
				("select level, date_format(datetime,'MMMM') as month, count(1) as total " + 
						"from big_logging_table group by level, month order by cast(first(date_format(datetime,'M')) as int), level");			

		bigResults.show(100);

		session.close();

	}

}
