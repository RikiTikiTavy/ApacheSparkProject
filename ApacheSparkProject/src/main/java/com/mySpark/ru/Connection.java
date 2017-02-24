package com.mySpark.ru;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;


public class Connection {

    //A name for the spark instance. Can be any string
    private static String appName = "V2 Maestros";
    //Pointer / URL to the Spark instance - embedded
    private static String sparkMaster = "local[2]";

    private static JavaSparkContext spContext = null;
    private static SparkSession sparkSession = null;
    private static String tempDir = "file:///c:/temp/spark-warehouse";

    private static void getConnection(String pathToWinutils) {

        if ( spContext == null) {
            //Setup Spark configuration
            SparkConf conf = new SparkConf()
                    .setAppName(appName)
                    .setMaster(sparkMaster);

            //Make sure you download the winutils binaries into this directory
            System.setProperty("hadoop.home.dir", pathToWinutils);

            //Create Spark Context from configuration
            spContext = new JavaSparkContext(conf);

            sparkSession = SparkSession
                    .builder()
                    .appName(appName)
                    .master(sparkMaster)
                    .config("spark.sql.warehouse.dir", tempDir)
                    .getOrCreate();
        }

    }

    public static SparkSession getSession(String pathToWinutils) {
        if ( sparkSession == null) {
            getConnection(pathToWinutils);
        }
        return sparkSession;
    }
}
