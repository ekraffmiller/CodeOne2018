/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.codeone2018.sparkk8sdemo;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

/**
 * Simple example of submitting a spark application via the SparkLauncher API
 * @author ellenk
 */
public class MySparkLauncher implements SparkAppHandle.Listener {
     private final CountDownLatch countDownLatch = new CountDownLatch(1);
  
    public static void main(String args[]) throws IOException, InterruptedException{
        MySparkLauncher launcher = new MySparkLauncher();
        launcher.submit(args);
    }
    public  void submit(String args[])  throws IOException, InterruptedException {
        String testCSV = "wine_reviews500.csv";
        String master = "k8s://https://192.168.99.100:8443";
         String appResource ="local:///opt/spark/jars/SparkK8SDemo-1.0-SNAPSHOT.jar";
         String image = "registry.hub.docker.com/ellenkraffmiller/text-analysis:latest";
         String sparkClass = "edu.harvard.codeone2018.sparkk8sdemo.SimpleSparkApp";
         
         SparkAppHandle handle = new SparkLauncher()
         .setAppResource(appResource)
         .setMainClass(sparkClass)
         .setMaster(master)
         .setSparkHome("/Applications/spark-2.3.2-bin-hadoop2.7")
     //    .setSparkHome("/Applications/spark-2.5.0-SNAPSHOT-bin-custom-spark")
         .setAppName("demo")
         .setConf("spark.app.name","demo")
         .setConf("spark.executor.instances","1")
         .setConf("spark.kubernetes.container.image",image)
         .setConf("spark.kubernetes.container.image.pullPolicy", "Always")
         .setConf("spark.kubernetes.authenticate.driver.serviceAccountName","spark")
         .setConf("spark.codeOne.demo.loadFile", testCSV)
         .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
         .addJar("http://central.maven.org/maven2/org/apache/hadoop/hadoop-azure/2.7.2/hadoop-azure-2.7.2.jar")
         .addJar("http://central.maven.org/maven2/com/microsoft/azure/azure-storage/3.1.0/azure-storage-3.1.0.jar")
         .setDeployMode("cluster")
      //   .addAppArgs(testCSV)
                
         .startApplication(this);
         
        boolean result = countDownLatch.await(1, TimeUnit.MINUTES);
        System.out.println(result? "completed!": "timed out");
     }
    
/**
* Callback method for changes to the Spark Job
*/
@Override
public void infoChanged(SparkAppHandle handle) {
    System.out.println("Info Changed.  State = [" + handle.getState() + "]");

}

/**
* Callback method for changes to the Spark Job's state
*/
@Override
public void stateChanged(SparkAppHandle handle) {

    System.out.println(" State Changed. State = [" + handle.getState() + "]");
    if (handle.getState().isFinal()) {
        countDownLatch.countDown();
    }
}

}
