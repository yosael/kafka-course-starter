package com.github.yosael.kafka.slasticsearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static RestHighLevelClient createClient(){

        // adding credentials
        String hostname = "kafka-starter-365045315.us-east-1.bonsaisearch.net";
        String username = "8dajzqyzjk";
        String password = "5mglpuib3h";

        // don't do if you run a local ES

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username,password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String,String> createConsumer(String topic){

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false"); //disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");

        // create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    //private JsonParser jsonParser = new JsonParser();

    private static String extractIDFromTweet(String tweetJson){

        return JsonParser.parseString(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();

    }

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = createClient();

        //String jsonString = "{ \"foo\": \"bar2\" }";

        KafkaConsumer<String,String> consumer = createConsumer("twitter_tweets");
        //Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( ()-> {

            logger.info("stopping application...");
            logger.info("shutting down consumer");
            logger.info("closing consumer...");
            consumer.close();
            logger.info("done!");

        }));

        while(true){
            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0

            Integer recordCount = records.count();
            logger.info("Received "+recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String,String> record: records){

                // 2 strategies
                // kafka generic ID
                // String id = record.topic() + "_" + record.particion() + "_" + record.offset();

                //twitter feed specific id
                try {
                    String id = extractIDFromTweet(record.value());

                    // where we insert data into ElasticSearch

                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id
                    ).source(record.value(), XContentType.JSON);

                    /*IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);*/
                    bulkRequest.add(indexRequest); // we add to our bulk request (takes no time)
                }
                catch (NullPointerException e){
                    logger.warn("skipping bad data: "+record.value());
                }

                //logger.info(indexResponse.getId());
            }
            if(recordCount>0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Commiting offsets... ");
                consumer.commitSync();
                logger.info("Offsets have been committed");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


        }

        //client.close();
    }

}
