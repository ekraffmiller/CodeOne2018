/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.harvard.codeone2018.sparkk8sdemo;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.mutable.WrappedArray;
/**
 *
 * @author ellenk
 */
public class SimpleSparkApp {
  
public static void main(String args[]) throws IOException, StorageException,InvalidKeyException, URISyntaxException {
     SimpleSparkApp app = new SimpleSparkApp();
     SparkSession session = SparkSession
                .builder()
                .appName("demo")
                .getOrCreate();
     app.run(session);
      
    }
 
    public void run(SparkSession session) throws IOException, StorageException, InvalidKeyException, URISyntaxException {
        // Convert raw text into feature vectors and vocabulary
        Dataset<Row> textRows = loadText(session);
        Dataset<Row> tokenized =  new RegexTokenizer().setPattern("\\W").setInputCol("text").setOutputCol("words").transform(textRows);
        Dataset<Row> filtered = new StopWordsRemover().setInputCol("words").setOutputCol("filtered").transform(tokenized);
        CountVectorizerModel cvModel = new CountVectorizer()
                .setInputCol("filtered")
                .setOutputCol("features").setMinDF(10).fit(filtered);
        Dataset<Row> features = cvModel.transform(filtered);
        String[] vocab =cvModel.vocabulary();
        
        // Define LDA model and apply it to features & generate topics
        LDA lda = new LDA().setK(10).setMaxIter(50);
        LDAModel model = lda.fit(features);
        
        // Extract topic terms and save results
        Dataset<Row> topics = model.describeTopics(10);
        topics.printSchema();
        List<Row> topicsList = topics.collectAsList();        
        saveResults(session, topicsList, vocab);
    }

   
    private Dataset<Row> loadText(SparkSession session) {
       String fromURI =session.sparkContext().getConf().get("spark.codeOne.demo.readFileURI");
       return session.read().format("csv")
                    .option("inferSchema", "true")
                    .option("header", "true")
                    .option("delimiter", ",")
                    .load(fromURI);
      
    }
   
    
    /**
     * Replace termIndices with term values, for more readablility
     * Write Results to fileURI, or local file
     * @param topicsList
     * @param vocab
     * @param writeFileURI 
     */
    private void saveResults(SparkSession session, List<Row> topicsList, String[] vocab) throws IOException,StorageException, InvalidKeyException, URISyntaxException {
        StringBuilder sb = new StringBuilder();
       
        topicsList.forEach(row -> {
            Integer[] indices = (Integer[]) ((WrappedArray<Integer>) row.getAs("termIndices")).array();
            String[] terms = new String[indices.length];
            for (int i = 0; i < indices.length; i++) {
                terms[i] = vocab[indices[i]];
            }
            sb.append(Arrays.toString(terms)).append(System.lineSeparator());
            System.out.println(Arrays.toString(terms));
        } );
        
        saveToCloudStorage(session,sb.toString());         
        
    }
    
    private void saveToCloudStorage(SparkSession session, String results)
            throws URISyntaxException, InvalidKeyException, StorageException, IOException {

        String blobAccountKey = session.conf().get("spark.codeOne.demo.storageKey");
        String storageConnectionString
                = "DefaultEndpointsProtocol=https;"
                + "AccountName=" + "consilience2" + ";"
                + "AccountKey=" + blobAccountKey + ";EndpointSuffix=core.windows.net";
        System.out.println("storage connection string: " + storageConnectionString);
        CloudStorageAccount.parse(storageConnectionString);

        CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
        CloudBlobContainer container = blobClient.getContainerReference("code-one-2018");
        container.createIfNotExists();
        CloudBlockBlob blob = container.getBlockBlobReference("LDAResults.txt");
        blob.uploadText(results);
    }

}
