/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.planner.plan;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.metastore.MetastoreUtil;
import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.planner.ExecutionPlanner;
import io.confluent.ksql.serde.WindowedSerde;
import io.confluent.ksql.structured.PhysicalPlan;
import io.confluent.ksql.structured.Table;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.SerDeUtil;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.Windowed;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Immutable
public class StructuredDataSourceNode
    extends PlanNode {

  private static final KeyValueMapper<String, GenericRow, KeyValue<String, GenericRow>> nonWindowedMapper = (key, row) -> {
    if (row != null) {
      row.getColumns().add(0, key);

    }
    return new KeyValue<>(key, row);
  };

  private static final KeyValueMapper<Windowed<String>, GenericRow, KeyValue<Windowed<String>, GenericRow>> windowedMapper = (key, row) -> {
    if (row != null) {
      row.getColumns().add(0,
          String.format("%s : Window{start=%d end=-}", key
              .key(), key.window().start()));

    }
    return new KeyValue<>(key, row);
  };

  private final WindowedSerde windowedSerde = new WindowedSerde();
  private final StructuredDataSource structuredDataSource;
  private final Schema schema;


  // TODO: pass in the "assignments" and the "outputs" separately
  // TODO: (i.e., get rid if the symbol := symbol idiom)
  @JsonCreator
  public StructuredDataSourceNode(@JsonProperty("id") final PlanNodeId id,
                                  @JsonProperty("structuredDataSource") final StructuredDataSource structuredDataSource,
                                  @JsonProperty("schema") Schema schema) {
    super(id);
    Objects.requireNonNull(structuredDataSource, "structuredDataSource can't be null");
    Objects.requireNonNull(schema, "schema can't be null");
    this.schema = schema;
    this.structuredDataSource = structuredDataSource;
  }

  public String getTopicName() {
    return structuredDataSource.getTopicName();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public Field getKeyField() {
    return structuredDataSource.getKeyField();
  }

  public StructuredDataSource getStructuredDataSource() {
    return structuredDataSource;
  }

  @Override
  public List<PlanNode> getSources() {
    return null;
  }

  @Override
  public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
    return visitor.visitStructuredDataSourceNode(this, context);
  }

  @Override
  public PhysicalPlan buildPhysical(final ExecutionPlanner executionPlanner,
                                    KsqlConfig ksqlConfig, final KafkaTopicClient kafkaTopicClient,
                                    MetastoreUtil metastoreUtil, FunctionRegistry functionRegistry, final Map<String, Object> props) {
    if (getTimestampField() != null) {
      int timestampColumnIndex = getTimeStampColumnIndex();
      ksqlConfig.put(KsqlConfig.KSQL_TIMESTAMP_COLUMN_INDEX, timestampColumnIndex);
    }

    Serde<GenericRow>
        genericRowSerde =
        SerDeUtil.getRowSerDe(getStructuredDataSource()
                .getKsqlTopic().getKsqlTopicSerDe(),
            SchemaUtil.removeImplicitRowTimeRowKeyFromSchema(
                getSchema()));

    if (getDataSourceType()
        == StructuredDataSource.DataSourceType.KTABLE) {
      final KsqlTable table = (KsqlTable) getStructuredDataSource();

      return createTable(
          executionPlanner,
          getAutoOffsetReset(props),
          table,
          genericRowSerde,
          SerDeUtil.getRowSerDe(table.getKsqlTopic().getKsqlTopicSerDe(),
              getSchema())
      );

    }

    return executionPlanner.stream(getStructuredDataSource().getKsqlTopic().getKafkaTopicName(), getKeyField());
//    return new SchemaKStream(getSchema(),
          // NOTE: This moves into ExecutionPlanner
//        builder
//            .stream(getStructuredDataSource().getKsqlTopic().getKafkaTopicName(),
//                Consumed.with(Serdes.String(), genericRowSerde))
//            .map(nonWindowedMapper)
//            .transformValues(new AddTimestampColumn()),
//        getKeyField(), new ArrayList<>(),
//        SchemaKStream.Type.SOURCE, functionRegistry);
  }

  private Topology.AutoOffsetReset getAutoOffsetReset(Map<String, Object> props) {
    if (props.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
      final String offestReset = props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).toString();
      if (offestReset
          .equalsIgnoreCase("EARLIEST")) {
        return Topology.AutoOffsetReset.EARLIEST;
      } else if (offestReset
          .equalsIgnoreCase("LATEST")) {
        return Topology.AutoOffsetReset.LATEST;
      }
    }
    return null;
  }

  private int getTimeStampColumnIndex() {
    String timestampFieldName = getTimestampField().name();
    if (timestampFieldName.contains(".")) {
      for (int i = 2; i < schema.fields().size(); i++) {
        Field field = schema.fields().get(i);
        if (field.name().contains(".")) {
          if (timestampFieldName.equals(field.name())) {
            return i - 2;
          }
        } else {
          if (timestampFieldName
              .substring(timestampFieldName.indexOf(".") + 1).equals(field.name())) {
            return i - 2;
          }
        }
      }
    } else {
      for (int i = 2; i < schema.fields().size(); i++) {
        Field field = schema.fields().get(i);
        if (field.name().contains(".")) {
          if (timestampFieldName.equals(field.name().substring(field.name().indexOf(".") + 1))) {
            return i - 2;
          }
        } else {
          if (timestampFieldName.equals(field.name())) {
            return i - 2;
          }
        }
      }
    }
    return -1;
  }

  private Table createTable(ExecutionPlanner planner, final Topology.AutoOffsetReset autoOffsetReset,
                             final KsqlTable ksqlTable,
                             final Serde<GenericRow> genericRowSerde,
                             final Serde<GenericRow> genericRowSerdeAfterRead) {
    if (ksqlTable.isWindowed()) {
      return planner.windowedTable(ksqlTable.getKsqlTopic().getKafkaTopicName(), autoOffsetReset);
        // NOTE: This would move into the implementation of ExecutionPlanner
      //      return table(builder.
//          .stream(ksqlTable.getKsqlTopic().getKafkaTopicName(),
//              Consumed.with(windowedSerde, genericRowSerde)
//                  .withOffsetResetPolicy(autoOffsetReset))
//          .map(windowedMapper)
//          .transformValues(new AddTimestampColumn()), windowedSerde, genericRowSerdeAfterRead);
    } else {
      return planner.table(ksqlTable.getKsqlTopic().getKafkaTopicName(), autoOffsetReset);
      // NOTE: This would move into the implementation of ExecutionPlanner
//      return table(planner
//              .stream(ksqlTable.getKsqlTopic().getKafkaTopicName(),
//                  Consumed.with(Serdes.String(), genericRowSerde)
//                      .withOffsetResetPolicy(autoOffsetReset))
//              .map(nonWindowedMapper)
//              .transformValues(new AddTimestampColumn()),
//          Serdes.String(), genericRowSerdeAfterRead);
    }
  }

  private <K> KTable table(final KStream<K, GenericRow> stream, final Serde<K> keySerde, final Serde<GenericRow> valueSerde) {
    return stream.groupByKey(Serialized.with(keySerde, valueSerde))
        .reduce((genericRow, newValue) -> newValue);
  }

  public StructuredDataSource.DataSourceType getDataSourceType() {
    return structuredDataSource.getDataSourceType();
  }

  public Field getTimestampField() {
    return structuredDataSource.getTimestampField();
  }
}
