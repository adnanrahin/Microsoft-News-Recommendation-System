## Microsoft News Data Set

#### Data Set
[MIND: Microsoft News Dataset](https://msnews.github.io/#getting-start)

#### Spark Parameters List
1. InputPath (Extract the above downloaded dataset and put in any folder such as `data-set` and input param should be `/User/data-set`)
2. Output (Output folder should be automatically created if not please create folder name `transform-data` and path should be look like this `/user/transform-data`)

### Build and Run Process
1. First Build the project, make sure you have maven, spark-cluster, jdk1.8 and scala 2.12.8 available.
2. Run This command `mvn clean install`
3. Once the build is done spin up the spark-cluster and submit the jobs from the command line.
4. Command is listed below.

#### Launch Command:
```
"/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home/bin/java" "-cp" 
"/opt/spark/spark-3.3.1-bin-hadoop2/conf/:/opt/spark/spark-3.3.1-bin-hadoop2/jars/*" 
"-Xmx1024M" "-Dspark.master=spark://Adnans-MBP:7077" 
"-Dspark.driver.supervise=false" "-Dspark.driver.memory=1G" 
"-Dspark.submit.deployMode=cluster" 
"-Dspark.jars=file:/Users/adnanrahin/source-code/scala/big-data/Microsoft-News-Recommendation-System/data-extract-engine/target/data-extract-engine-1.0-SNAPSHOT.jar" 
"-Dspark.app.name=DataExtractEngineSparkSubmit" 
"-Dspark.cores.max=8" "-Dspark.executor.memory=2G" "-Dspark.submit.pyFiles=" "-Dspark.app.submitTime=1671865336577" "-Dspark.rpc.askTimeout=10s" "-Dspark.executor.cores=2" "-Dspark.driver.cores=1" "org.apache.spark.deploy.worker.DriverWrapper" "spark://Worker@192.168.1.64:54491" "/opt/spark/spark-3.3.1-bin-hadoop2/work/driver-20221224020217-0001/data-extract-engine-1.0-SNAPSHOT.jar" "org.microsoft.news.data.extractor.DataExtractEngine" 
"/Users/adnanrahin/source-code/scala/big-data/Microsoft-News-Recommendation-System/data-set" 
"/Users/adnanrahin/source-code/scala/big-data/Microsoft-News-Recommendation-System/transform-data"
```