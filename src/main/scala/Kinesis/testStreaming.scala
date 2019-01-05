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

object testStreaming {
  def main(args: Array[String]): Unit = {

    val appName = "KinesisStreaming"
    val streamName = "logStream"
    val endpointUrl = "https://kinesis.us-east-1.amazonaws.com"
    val region = "us-east-1"

    val endpoint = new EndpointConfiguration(endpointUrl, region)
    val credentials = new DefaultAWSCredentialsProviderChain()
    require(credentials != null,
      "No AWS credentials found. Please specify credentials using one of the methods specified")

    //StreamingLogger.setStreamingLogLevels(Level.WARN)

    val builder = AmazonKinesisClient.builder()
    builder.setCredentials(credentials)
    builder.setEndpointConfiguration(endpoint)
    println("Build successful...")

    val kinesisClient = builder.build()
    val streamInfo = kinesisClient.describeStream(streamName).getStreamDescription()
    val numShards = streamInfo.getShards.size()
    println("Stream Info:\n" + streamInfo.toString)

    val numStreams = numShards
    val batchInterval = Seconds(2)
    val kinesisCheckpointInterval = batchInterval

    val sparkConfig = new SparkConf().setAppName(appName).setMaster("local[2]")
    val sc = new SparkContext(sparkConfig)
    val ssc = new StreamingContext(sc, batchInterval)

    val kinesisStreams = (0 until numStreams).map { i =>
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

    val unionStreams = ssc.union(kinesisStreams)
    unionStreams.print()

    ssc.start()
    ssc.awaitTermination()

  }

}

//private[streaming] object StreamingLogger extends Logging {
//  def setStreamingLogLevels(level: Level) {
//    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
//    if (!log4jInitialized) {
//      logInfo("Setting log level to [WARN] for streaming application.")
//      Logger.getRootLogger.setLevel(level)
//    }
//  }
//}
