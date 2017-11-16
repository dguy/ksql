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

package io.confluent.ksql.rest.server.computation;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.query.QueryIdProvider;
import io.confluent.ksql.util.Pair;

import static org.easymock.EasyMock.anyLong;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;


@RunWith(EasyMockRunner.class)
public class CommandStoreTest {

  private static final String COMMAND_TOPIC = "command";
  @Mock(type = MockType.NICE)
  private Consumer<CommandId, Command> commandConsumer;
  @Mock(type = MockType.NICE)
  private Producer<CommandId, Command> commandProducer;

  private final QueryIdProvider queryIdProvider = new QueryIdProvider();

  @SuppressWarnings("unchecked")
  @Test
  public void shouldRemovePriorCommandsThatHaveBeenTombstoned() {
    final CommandId idTwo = new CommandId(CommandId.Type.TABLE, "two", CommandId.Action.CREATE);
    final Command commandTwo = new Command("some other statement", Collections.emptyMap(), queryIdProvider.next());

    final ConsumerRecords<CommandId, Command> records = new ConsumerRecords<>(Collections.singletonMap(new TopicPartition("topic", 0), Arrays.asList(
        new ConsumerRecord<>("topic", 0, 0, new CommandId(CommandId.Type.TOPIC, "one", CommandId.Action.CREATE),
            new Command("some statement", Collections.emptyMap(), queryIdProvider.next())),
        new ConsumerRecord<>("topic", 0, 0, idTwo,
            commandTwo),
        new ConsumerRecord<>("topic", 0, 0, new CommandId(CommandId.Type.TOPIC, "one", CommandId.Action.CREATE),
            null)
    )));

    EasyMock.expect(commandConsumer.partitionsFor(COMMAND_TOPIC)).andReturn(Collections.emptyList());

    EasyMock.expect(commandConsumer.poll(anyLong())).andReturn(records)
        .andReturn(new ConsumerRecords<>(Collections.emptyMap()));
    EasyMock.replay(commandConsumer);

    final CommandStore command = new CommandStore(COMMAND_TOPIC, commandConsumer, commandProducer, new CommandIdAssigner(new MetaStoreImpl()), queryIdProvider);
    final List<Pair<CommandId, Command>> priorCommands = command.getPriorCommands();
    assertThat(priorCommands, equalTo(Collections.singletonList(new Pair<>(idTwo, commandTwo))));
  }

  @Test
  public void shouldOnlyHaveLatestCommandForCommandId() {
    final CommandId commandId = new CommandId(CommandId.Type.TOPIC, "one", CommandId.Action.CREATE);
    final Command latestCommand = new Command("a new statement", Collections.emptyMap(), queryIdProvider.next());
    final ConsumerRecords<CommandId, Command> records = new ConsumerRecords<>(Collections.singletonMap(new TopicPartition("topic", 0), Arrays.asList(
        new ConsumerRecord<>("topic", 0, 0, commandId,
            new Command("some statement", Collections.emptyMap(), queryIdProvider.next())),
        new ConsumerRecord<>("topic", 0, 0, commandId,
            latestCommand))
    ));

    EasyMock.expect(commandConsumer.partitionsFor(COMMAND_TOPIC)).andReturn(Collections.emptyList());

    EasyMock.expect(commandConsumer.poll(anyLong())).andReturn(records)
        .andReturn(new ConsumerRecords<>(Collections.emptyMap()));
    EasyMock.replay(commandConsumer);

    final CommandStore command = new CommandStore(COMMAND_TOPIC, commandConsumer, commandProducer, new CommandIdAssigner(new MetaStoreImpl()), queryIdProvider);
    final List<Pair<CommandId, Command>> priorCommands = command.getPriorCommands();
    assertThat(priorCommands, equalTo(Collections.singletonList(new Pair<>(commandId, latestCommand))));
  }

  @Test
  public void shouldSendTombstoneForPriorDropCommands() {
    final CommandId commandId = new CommandId(CommandId.Type.TOPIC, "one", CommandId.Action.DROP);
    final Command latestCommand = new Command("a new statement", Collections.emptyMap(), queryIdProvider.next());
    final ConsumerRecords<CommandId, Command> records = new ConsumerRecords<>(Collections.singletonMap(new TopicPartition("topic", 0), Collections.singletonList(
        new ConsumerRecord<>("topic", 0, 0, commandId,
            latestCommand))
    ));

    EasyMock.expect(commandConsumer.partitionsFor(COMMAND_TOPIC)).andReturn(Collections.emptyList());

    EasyMock.expect(commandConsumer.poll(anyLong())).andReturn(records)
        .andReturn(new ConsumerRecords<>(Collections.emptyMap()));

    EasyMock.expect(commandProducer.send(new ProducerRecord<>(COMMAND_TOPIC, commandId, null))).andReturn(null);
    EasyMock.replay(commandConsumer, commandProducer);

    final CommandStore command = new CommandStore(COMMAND_TOPIC, commandConsumer, commandProducer, new CommandIdAssigner(new MetaStoreImpl()), queryIdProvider);
    final List<Pair<CommandId, Command>> priorCommands = command.getPriorCommands();
    assertThat(priorCommands, equalTo(Collections.emptyList()));
    EasyMock.verify(commandProducer);
  }

}