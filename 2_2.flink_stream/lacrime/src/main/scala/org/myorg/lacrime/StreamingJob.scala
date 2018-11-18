/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.lacrime

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.api.common.serialization.SimpleStringSchema
import scala.util.parsing.json.JSON
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // connect with Kafka
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","localhost:9092")
    val stream = env.addSource(
      new FlinkKafkaConsumer09[String](
        "la-crime",
        new SimpleStringSchema(),
        prop)
    )

    // loading stream
    val crimes = stream
        .map( str => JSON.parseFull(str) )
        .map( obj => obj.get.asInstanceOf[Map[String, Any]])

    // calculating the number of crime for each type
    crimes
      .map{ c => CrimeCount( c("Crime Code Description").asInstanceOf[String], 1)}
      .keyBy("crime")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")
      .print()
      .setParallelism(1)

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
  case class CrimeCount(crime: String, count: Long)
}
