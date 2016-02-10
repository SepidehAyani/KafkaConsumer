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

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by sepidehayani on 2/10/16.
 * This SimpleConsumer class is used to fetch a
 * message from a specific topic and consume it,
 * assuming that there is a single partition within the topic.
 */

public class SimpleConsumer {

  //If instance variable is set final its value can not be changed
  private final ConsumerConnector consumer;
  private final String topic;

  public SimpleConsumer (String zookeeper, String groupId, String topic) {
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
    this.topic = topic;
  }

  private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {

    //Defining properties
    Properties props = new Properties();

      /*This property specifies the Zookeeper connection string,
      < hostname:port/chroot/path>. Kafka uses Zookeeper to
      store offsets of messages consumed for a specific topic
      and partition by the consumer group. /chroot/path defines
      the data location in a global zookeeper namespace.
       */
    props.put("zookeeper.connect", zookeeper);

      /*This property defines a unique identity for
      the set of consumers within the same consumer group.
      */
    props.put("group.id", groupId);

      /*This property defines the time (in milliseconds)
      for a Kafka consumer to wait for a Zookeeper pulse before
      it is declared dead and rebalance is initiated.
       */
    props.put("zookeeper.session.timeout.ms", "500");

      /*This property defines the time it takes to sync a
      Zookeeper follower with the Zookeeper leader (in milliseconds).
       */
    props.put("zookeeper.sync.time.ms", "250");

      /*This property defines the frequency (in milliseconds)
      for the consumed offsets to get committed to ZooKeeper.
       */
    props.put("auto.commit.interval.ms", "1000");

    return new ConsumerConfig(props);

  }

  public void testConsumer() {

    Map<String, Integer> topicMap = new HashMap<String, Integer>();

    // 1 represents the single thread ~Defining single thread for topic

    topicMap.put(topic, new Integer(1));

    Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer.createMessageStreams(topicMap);
    System.out.println("creating consumer message");
    // Get the list of message streams for each topic, using the default decoder.
    List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap.get(topic);
    System.out.println("stream mapper");
    for (final KafkaStream<byte[], byte[]> stream : streamList) {

      ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();

      while (consumerIte.hasNext())
        System.out.println("Message from Single Topic :: " + new String(consumerIte.next().message()));
    }
    if (consumer != null)
      consumer.shutdown();
  }

  public static void main(String[] args) {

    if (args.length < 3) {
      System.err.println("Usage:SimpleConsumer <Zookeeper> <groupId> <Kafka-topic>");
      System.exit(-1);
    }
    String zooKeeper = args[0];
    String groupId = args[1];
    String topic = args[2];
    System.out.println("zooKeeper" + zooKeeper);
    System.out.println("groupId" + groupId);
    System.out.println("topic" + topic);

    SimpleConsumer  simpleConsumer = new SimpleConsumer(zooKeeper, groupId, topic);
    simpleConsumer.testConsumer();
  }

}
