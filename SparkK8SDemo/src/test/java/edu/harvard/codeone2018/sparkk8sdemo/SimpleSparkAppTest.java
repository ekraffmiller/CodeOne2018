/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.codeone2018.sparkk8sdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author ellenk
 */
public class SimpleSparkAppTest {
    
    public SimpleSparkAppTest() {
    }

    /**
     * Test of main method, of class SimpleSparkApp.
     */
 //  @org.junit.Test
    public void testRun() {
           System.setProperty("spark.master", "local[2]");
           SparkConf conf = new SparkConf();
           String[] azureJars = { 
               "http://central.maven.org/maven2/org/apache/hadoop/hadoop-azure/2.7.2/hadoop-azure-2.7.2.jar",
               "http://central.maven.org/maven2/com/microsoft/azure/azure-storage/3.1.0/azure-storage-3.1.0.jar"};
       
         conf.setJars(azureJars);
         SparkSession session = SparkSession
                .builder()
                .appName("Process documents")
                .config(conf)
                .getOrCreate();
      
        SimpleSparkApp app = new SimpleSparkApp();
        String readFileURI =  "wasb://code-one-2018@consilience2.blob.core.windows.net/wine_review500.csv";
       
        app.run(session,readFileURI);
          
    }
    
}
