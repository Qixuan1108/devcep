package org.example;

import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import at.datasciencelabs.pattern.*;

import java.util.List;
import java.util.Map;

public class DslCep {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment DslEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DslEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DslEnv.setParallelism(1);

        DataStream<Event> eventDataStream = DslEnv.addSource(new EventSource());

        PatternStream<Event> patternStream = Dsl.compile("%NO_SKIP\nA(eventType = 'A' and value < 20) -> " +
                        "B(eventType = 'B' and startTimeStamp - A.startTimeStamp > 2000) within 3s", eventDataStream);


        OutputTag<String> outputTag = new OutputTag<String>("myOutput"){};

        SingleOutputStreamOperator<String> resultStream = patternStream.select(outputTag, new PatternTimeoutFunction<Event, String>() {
            @Override
            public String timeout(Map<String, List<Event>> pattern, long timeoutTimestamp) throws Exception {
                StringBuilder timeoutResult = new StringBuilder();
                for(List<Event> eventList : pattern.values()){
                    for(Event event : eventList){
                        timeoutResult.append(event.toString());
                    }
                }
                return "timeout Pattern:" + timeoutResult.toString();
            }
        }, new PatternSelectFunction<Event, String>() {
            @Override
            public String select(Map<String, List<Event>> map) throws Exception {
                StringBuilder result = new StringBuilder();
                for(List<Event> eventList : map.values()){
                    for(Event event : eventList){
                        result.append(event.toString());
                    }
                }
                return "Matched Pattern" + result.toString();
            }
        });

        resultStream.print();

        DataStream<String> sideOutput = resultStream.getSideOutput(outputTag);
        sideOutput.print();

        System.out.println("Pattern Match Begin");
        DslEnv.execute("My Test DslCEP");
    }
}
