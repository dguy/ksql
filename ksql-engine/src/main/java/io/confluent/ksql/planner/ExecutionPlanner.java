/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.ksql.planner;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Windowed;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.structured.Stream;
import io.confluent.ksql.structured.Table;

public interface ExecutionPlanner {
  Stream stream(String kafkaTopicName, Field windowedGenericRowConsumed);

  Table windowedTable(String kafkaTopicName, Topology.AutoOffsetReset autoOffsetReset);

  Table table(String kafkaTopicName, Topology.AutoOffsetReset autoOffsetReset);
}
