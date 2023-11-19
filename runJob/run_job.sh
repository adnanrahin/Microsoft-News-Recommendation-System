#!/bin/bash

SPARK_HOME=$SPARK_HOME
APP_JAR="/home/rahin/source-code/spark/Microsoft-News-Recommendation-System/data-extract-engine/target/data-extract-engine-1.0-SNAPSHOT.jar"
INPUT_PATH="/sandbox/storage/data/microsoft-news-recommendation-data/data-set/"
OUTPUT_PATH="/sandbox/storage/data/microsoft-news-recommendation-data/transform-data"

$SPARK_HOME/bin/spark-submit \
    --master spark://dev-server01:7077 \
    --deploy-mode cluster \
    --class org.microsoft.news.data.extractor.DataExtractEngine \
    --name DataExtractEngineSparkSubmit \
    --driver-memory 1G \
    --driver-cores 1 \
    --executor-memory 2G \
    --executor-cores 2 \
    --total-executor-cores 8 \
    $APP_JAR $INPUT_PATH $OUTPUT_PATH

