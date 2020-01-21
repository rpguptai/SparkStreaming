package com.versh.sparkML;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FeatureSelection {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkSession session = SparkSession.builder()
				.appName("FeatureSelection") 
				.master("local[*]")
				.getOrCreate();

		Logger.getLogger("org.apache").setLevel(Level.ERROR);

		Dataset<Row> csvData = session.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/kc_house_data.csv");		

		csvData.describe().show();

		csvData = csvData.drop("id","date", "waterfront","view","condition","grade","yr_renovated","zipcode","lat","long");
		
		//corelation between feature and label...whatever is close to zero means not near to +1 or -1.. drop it
		for (String col : csvData.columns() ) {
			System.out.println("The correlation between the price and " + col + " is " + csvData.stat().corr("price", col));
		}
		
		csvData = csvData.drop("sqft_lot","sqft_lot15","yr_built","sqft_living15");
		
		//corelation between colums in feature ... remove highly corelated  column (choose one... which has higher corelation with price)
		// google corelation matrix...
		for (String col1 : csvData.columns()) {
			for (String col2 : csvData.columns()) {
				System.out.println("The correlation between " + col1 + " and " + col2 + " is " + csvData.stat().corr(col1, col2));
			}
		}
		
	}

}
