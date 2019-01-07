package spark.applications.jobs;

import org.slf4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import spark.applications.util.data.Event_MB;

import java.io.Serializable;

public abstract class AbstractJob implements Serializable {
    //use spout as directKafkaStream

    protected final SparkConf config;
    protected final int batch;
    protected final int count_number;
    public JavaPairInputDStream<String, String> spout;
    public JavaPairDStream<String, String> kafkaStream;
    public JavaPairDStream<String, Event_MB> sink;
    protected String topologyName;

    public AbstractJob(String topologyName, SparkConf config) {
        this.topologyName = topologyName;
        this.config = config;
        batch = config.getInt("batch", 1);
        count_number = config.getInt("count_number", 1);
    }


    public abstract void initialize(JavaStreamingContext ssc);

    //build job to get the last JavaDStream.
    //with input as the first JavaDStream
    public abstract AbstractJob buildJob(JavaStreamingContext ssc);

    public abstract Logger getLogger();

    public abstract String getConfigPrefix();
}