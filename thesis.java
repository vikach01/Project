package com.vikbmethesis;

import java.util.Properties;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.examples.statemachine.event.Alert;
import org.apache.flink.streaming.examples.statemachine.generator.EventsGeneratorSource;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.util.Collector;

import akka.actor.FSM.Event;
import akka.actor.FSM.State;

public class StateMachineExample {
	
  public static void main(String[] args) throws Exception {
    EventsGeneratorSource eventsGeneratorSource;
    
    System.out.println("Usage with Kafka: StateMachineExample --kafka-topic <topic> [--brokers <brokers>]");
    System.out.println("Options for both the above setups: ");
    
    System.out.println();
    
    ParameterTool params = ParameterTool.fromArgs(args);
    
    if (params.has("kafka-topic")) {
      String kafkaTopic = params.get("kafka-topic");
      String brokers = params.get("brokers", "localhost:9092");
      System.out.printf("Reading from kafka topic %s @ %s\n", new Object[] { kafkaTopic, brokers });
      System.out.println();
      Properties kafkaProps = new Properties();
      kafkaProps.setProperty("bootstrap.servers", brokers);
      
      FlinkKafkaConsumer010<Event> kafka = new FlinkKafkaConsumer010(kafkaTopic, (DeserializationSchema)new EventDeSerializer(), kafkaProps);
      kafka.setStartFromLatest();
      kafka.setCommitOffsetsOnCheckpoints(false);
      
      FlinkKafkaConsumer010<Event> flinkKafkaConsumer0101 = kafka;
    } else {
      double errorRate = params.getDouble("error-rate", 0.0D);
      int sleep = params.getInt("sleep", 1);
      System.out.printf("Using standalone source with error rate %f and sleep delay %s millis\n", new Object[] { Double.valueOf(errorRate), Integer.valueOf(sleep) });
      System.out.println();
      eventsGeneratorSource = new EventsGeneratorSource(errorRate, sleep);
    } 
    
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(2000L);
    
    String stateBackend = params.get("backend", "memory");
    if ("file".equals(stateBackend)) {
      String checkpointDir = params.get("checkpoint-dir");
      boolean asyncCheckpoints = params.getBoolean("async-checkpoints", false);
      env.setStateBackend((AbstractStateBackend)new FsStateBackend(checkpointDir, asyncCheckpoints));
    } else if ("rocks".equals(stateBackend)) {
      String checkpointDir = params.get("checkpoint-dir");
      boolean incrementalCheckpoints = params.getBoolean("incremental-checkpoints", false);
      env.setStateBackend((AbstractStateBackend)new RocksDBStateBackend(checkpointDir, incrementalCheckpoints));
    } 
    String outputFile = params.get("output");
    env.getConfig().setGlobalJobParameters((ExecutionConfig.GlobalJobParameters)params);
    DataStreamSource dataStreamSource = env.addSource((SourceFunction)eventsGeneratorSource);
    
    SingleOutputStreamOperator singleOutputStreamOperator = dataStreamSource.keyBy(Event::sourceAddress).flatMap((FlatMapFunction)new StateMachineMapper());
    if (outputFile == null) {
      singleOutputStreamOperator.print();
    } else {
      singleOutputStreamOperator
        .writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE)
        .setParallelism(1);
    } 
    env.execute("State machine job");
  }
  
  static class StateMachineMapper extends RichFlatMapFunction<Event, Alert> {
    private ValueState<State> currentState;
    
    public void open(Configuration conf) {
      this.currentState = getRuntimeContext().getState(new ValueStateDescriptor("state", State.class));
    }
    
    public void flatMap(Event evt, Collector<Alert> out) throws Exception {
      State state = (State)this.currentState.value();
      if (state == null)
        state = State.Initial; 
      State nextState = state.transition(evt.type());
      if (nextState == State.InvalidTransition) {
        out.collect(new Alert(evt.sourceAddress(), state, evt.type()));
      } else if (nextState.isTerminal()) {
        this.currentState.clear();
      } else {
        this.currentState.update(nextState);
      } 
    }
  }
}