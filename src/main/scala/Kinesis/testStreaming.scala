package Kinesis

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kinesis.{KinesisInputDStream, KinesisInitialPositions}

import org.apache.log4j.{Level, Logger}

object testStreaming extends Logging {
  def main(args: Array[String]): Unit = {

    //Kinesis Configurations
    val appName = "FYPKinesisStreaming"
    val streamName = "logStream"
    val endpointUrl = "https://kinesis.us-east-1.amazonaws.com"
    val region = "us-east-1"

    //AWS Configurations
    val endpoint = new EndpointConfiguration(endpointUrl, region)
    val credentials = new DefaultAWSCredentialsProviderChain()
    require(credentials != null,
      "No AWS credentials found. Please specify credentials using one of the methods specified")

    //Log Configurations
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      logInfo("Setting log level to [WARN] for streaming application.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    //Configure Kinesis Client
    val builder = AmazonKinesisClient.builder()
    builder.setCredentials(credentials)
    builder.setEndpointConfiguration(endpoint)
    println("Build successful, Getting Stream Info:")

    //Verify Stream
    val kinesisClient = builder.build()
    val streamInfo = kinesisClient.describeStream(streamName).getStreamDescription()
    val numShards = streamInfo.getShards.size()
    println(streamInfo.toString)

    //Configure Spark
    val batchInterval = Seconds(5)
    val kinesisCheckpointInterval = batchInterval

    val sparkConfig = new SparkConf().setAppName(appName).setMaster("local[2]")
    val sc = new SparkContext(sparkConfig)
    val ssc = new StreamingContext(sc, batchInterval)

    //Build Kinesis Stream
    val kinesisStreams = (0 until numShards).map { i =>
      KinesisInputDStream.builder
        .streamingContext(ssc)
        .streamName(streamName)
        .endpointUrl(endpointUrl)
        .regionName(region)
        .initialPosition(new KinesisInitialPositions.Latest())
        .checkpointAppName(appName)
        .checkpointInterval(kinesisCheckpointInterval)
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .build()
    }

    //Code
    val unionStreams = ssc.union(kinesisStreams)
    val logStream = unionStreams.flatMap(byteArray => new String(byteArray))
    logStream.print()
    //End of Code

    println("All set, Spark starting...")
    ssc.start()
    ssc.awaitTermination()

  }

}

