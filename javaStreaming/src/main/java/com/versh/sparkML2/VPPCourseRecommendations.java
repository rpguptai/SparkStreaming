package com.versh.sparkML2;

import static org.apache.spark.sql.functions.col;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class VPPCourseRecommendations {

	public static void main(String[] args) {
		// TODO Auto-generated method stub


		SparkSession session = SparkSession.builder()
				.appName("VPPCourseRecommendations") 
				.master("local[*]")
				.getOrCreate();

		Logger.getLogger("org.apache").setLevel(Level.ERROR);

		Dataset<Row> csvData = session.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/VPPcourseViews.csv");	

		csvData = csvData.withColumn("proportionWatched", col("proportionWatched").multiply(100));

		csvData.show();

		/*
		 * Dataset<Row>[] trainingAndHoldoutData = csvData.randomSplit(new double[]
		 * {0.8,0.2}); Dataset<Row> trainingData = trainingAndHoldoutData[0];
		 * Dataset<Row> holdoutData = trainingAndHoldoutData[1];
		 * 
		 * ALS als = new ALS() .setMaxIter(10) .setRegParam(0.1) .setUserCol("userId")
		 * .setItemCol("courseId") .setRatingCol("proportionWatched");
		 * 
		 * ALSModel model = als.fit(trainingData);
		 * 
		 * Dataset<Row> prediction = model.transform(holdoutData); prediction.show();
		 */

		ALS als = new ALS() .setMaxIter(10) 
				.setRegParam(0.1) 
				.setUserCol("userId")
				.setItemCol("courseId") 
				.setRatingCol("proportionWatched");

		ALSModel model = als.fit(csvData);
		
		model.setColdStartStrategy("drop");

		/*
		 * Dataset<Row> userRecs = model.recommendForAllUsers(5);
		 * 
		 * List<Row> userRecsList = userRecs.takeAsList(5);
		 * 
		 * for (Row r : userRecsList) { int userId = r.getAs(0); String recs =
		 * r.getAs(1).toString(); System.out.println("User " + userId +
		 * " we might want to recommend " + recs);
		 * System.out.println("This user has already watched: ");
		 * csvData.filter("userId = " + userId).show(); }
		 */	

		
		Dataset<Row> testData = session.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/VPPcourseViewsTest.csv");
		
		model.transform(testData).show();
		model.recommendForUserSubset(testData, 5).show();
		
		
		session.close();
	}

}
