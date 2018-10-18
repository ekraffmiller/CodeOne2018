
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
 * Simple text clustering example, reads CSV from Azure Blob Storage, runs LDA Topic Analysis,
 * and saves the results to Azure.
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
    /**
     * Main method - load CSV text, use LDA Topic analysis to generate topic terms
     * @param session
     * @throws IOException
     * @throws StorageException
     * @throws InvalidKeyException
     * @throws URISyntaxException 
     */
    public void run(SparkSession session) throws IOException, StorageException, InvalidKeyException, URISyntaxException {
        //
        // Convert raw text into feature vectors and vocabulary
        //
        Dataset<Row> textRows = loadText(session);
        Dataset<Row> tokenized =  new RegexTokenizer().setPattern("\\W").setInputCol("text").setOutputCol("words").transform(textRows);
        Dataset<Row> filtered = new StopWordsRemover().setInputCol("words").setOutputCol("filtered").transform(tokenized);
        CountVectorizerModel cvModel = new CountVectorizer()
                .setInputCol("filtered")
                .setOutputCol("features").setMinDF(10).fit(filtered);
        Dataset<Row> features = cvModel.transform(filtered);
        String[] vocab =cvModel.vocabulary();
        
        //
        // Define LDA model and apply it to features to generate topics
        //
        LDA lda = new LDA().setK(10).setMaxIter(50);
        LDAModel model = lda.fit(features);
        // Extract topic terms and save results
        Dataset<Row> topics = model.describeTopics(10);
        List<Row> topicsList = topics.collectAsList();        
        saveResults(session, topicsList, vocab);
    }

   /**
    * Read CSV file from Azure blob storage into a Spark Dataframe 
    * @param session
    * @return 
    */
    private Dataset<Row> loadText(SparkSession session) {
       String fromURI =session.sparkContext().getConf().get("spark.codeOne.demo.readFileURI");
       return session.read().format("csv")
                    .option("inferSchema", "true")
                    .option("header", "true")
                    .option("delimiter", ",")
                    .load(fromURI);
      
    }
   
    
    /**
     * Replace LDA topic termIndices with term values, for more readablility
     * Write Results to Azure Blob Storage
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
        } );
        
        saveToCloudStorage(session,sb.toString());         
        
    }
    
    /**
     * Make connection to Azure Storage account, and upload the text results
     * to Blob storage, in LDAResults.txt
     * @param session
     * @param results
     * @throws URISyntaxException
     * @throws InvalidKeyException
     * @throws StorageException
     * @throws IOException 
     */
    private void saveToCloudStorage(SparkSession session, String results)
            throws URISyntaxException, InvalidKeyException, StorageException, IOException {

        String blobAccountKey = session.conf().get("spark.codeOne.demo.storageKey");
            String storageConnectionString
                    = "DefaultEndpointsProtocol=https;"
                    + "AccountName=" + "consilience2" + ";"
                    + "AccountKey=" + blobAccountKey + ";EndpointSuffix=core.windows.net";
            CloudStorageAccount.parse(storageConnectionString);

            CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
            CloudBlobContainer container = blobClient.getContainerReference("code-one-2018");
            container.createIfNotExists();
            CloudBlockBlob blob = container.getBlockBlobReference("LDAResults.txt");
            blob.uploadText(results);
        }

}
