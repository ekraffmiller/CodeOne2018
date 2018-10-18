# CodeOne2018

Demo for Running Spark Application on Kubernetes.  Performs simple text analysis on CSV file. Reads and writes to an Azure Blob.

Runtime arguments for SparkLauncher:  
-Dmaster="k8s://{your kubernetes url}"    
-DreadFileURI="wasb://code-one-2018@consilience2.blob.core.windows.net/wine_reviews500.csv"   
-DdemoImage="registry.hub.docker.com/ellenkraffmiller/text-analysis:latest"  
-DstorageKey="{azure storage account key}"  
         
