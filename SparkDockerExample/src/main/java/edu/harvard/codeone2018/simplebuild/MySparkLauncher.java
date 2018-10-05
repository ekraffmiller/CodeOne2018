/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.codeone2018.simplebuild;

import java.io.IOException;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

/**
 *
 * @author ellenk
 */
public class MySparkLauncher {
    
    public static void main(String args[])  throws IOException, InterruptedException {
    String master = "k8s://https://192.168.99.100:8443";
         String appResource ="local:///opt/spark/jars/SparkDockerExample-1.0-SNAPSHOT.jar";
         String image = "registry.hub.docker.com/ellenkraffmiller/wordcount:latest";
         String sparkClass = "edu.harvard.codeone2018.simplebuild.SimpleSparkApp";
         
        // System.setProperty("SPARK_HOME", "/Applications/spark-2.3.2-bin-hadoop2.7");
         SparkAppHandle handle = new SparkLauncher()
         .setAppResource(appResource)
         .setMainClass(sparkClass)
         .setMaster(master)
        .setSparkHome("/Applications/spark-2.3.2-bin-hadoop2.7")
         .setAppName("wordcount")
         .setConf("spark.app.name","wordcount")
         .setConf("spark.executor.instances","1")
         .setConf("spark.kubernetes.container.image",image)
         .setConf("spark.kubernetes.container.image.pullPolicy", "Always")
         .setConf("spark.kubernetes.authenticate.driver.serviceAccountName","spark")
         .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
         .setDeployMode("cluster")
         .addAppArgs("100")
         .startApplication();
       // Use handle API to monitor / control application.
        boolean complete = false;
        while(!complete) {
            Thread.sleep(2000);
          SparkAppHandle.State s = handle.getState();
       //   
            if (s.equals(SparkAppHandle.State.LOST)|| s.equals(SparkAppHandle.State.FAILED) || s.equals(SparkAppHandle.State.FINISHED) || s.equals(SparkAppHandle.State.KILLED)) {
                 System.out.println("state changed to: " +s.name());
                 complete=true;
            }  
        }
        System.out.println("completed!");
     }
}
