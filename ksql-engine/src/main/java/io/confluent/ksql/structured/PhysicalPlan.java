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
package io.confluent.ksql.structured;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.util.Pair;

public interface PhysicalPlan {
  Schema getSchema();

  PhysicalPlan withSchema(Schema schema);

  PhysicalPlan filter(Expression havingExpressions);

  PhysicalPlan selectKey(KeyValueMapper<String, GenericRow, String> mapper);

  PhysicalPlan select(List<Pair<String,Expression>> finalSelectExpressions);

  GroupedStream groupByKey(Serde<String> string, Serde<GenericRow> genericRowSerde);

  Field keyField();

  PhysicalPlan withOutputNode(OutputNode ksqlBareOutputNode);

  PhysicalPlan withLimit(Optional<Integer> limit);

  PhysicalPlan into(String kafkaTopicName, Serde<GenericRow> rowSerDe, Set<Integer> rowkeyIndexes);
}
