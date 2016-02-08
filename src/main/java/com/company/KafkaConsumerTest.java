/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.company;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;

import org.apache.commons.cli.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

public class KafkaConsumerTest {

  public static void main(String[] args) {

    Options options = new Options();
    options.addOption("topic", true, "Reading the topic");
    options.addOption("brokerList", true, "Reading the broker list");

    StreamingExamples.setStreamingLogLevels();

    String brokerList = args[0];
    String topic = args[1];

    // Create context with a 2 seconds batch interval
    SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaConsumer");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

    HashSet<String> topicSet = new HashSet<String>(Arrays.asList(topic.split(",")));
    HashMap<String, String> kafkaParams = new HashMap<String, String>();
    kafkaParams.put("bootstrap.servers", brokerList);

    // Create direct kafka stream with brokers and topics
    //DStream Print

    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(

            String.class,
            String.class,
            topic,
            brokerList,

            );

    messages.print();

    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }
}