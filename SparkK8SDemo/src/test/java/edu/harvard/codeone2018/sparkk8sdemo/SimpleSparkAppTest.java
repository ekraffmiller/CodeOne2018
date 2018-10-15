/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.codeone2018.sparkk8sdemo;

import com.microsoft.azure.storage.StorageException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
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
    public void testRun() throws IOException, StorageException,URISyntaxException, InvalidKeyException{
           System.setProperty("spark.master", "local[2]");
           SparkConf conf = new SparkConf();
          
         SparkSession session = SparkSession
                .builder()
                .appName("Process documents")
                .config(conf)
                .getOrCreate();
      
        SimpleSparkApp app = new  SimpleSparkApp();
        
        app.run(session);
          
    }
    
}
