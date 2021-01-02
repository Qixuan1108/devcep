package org.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;

import at.datasciencelabs.pattern.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class GraphCep {
    static TimeGraph timeGraph = GraphCep.createTimeGraph();

    public static TimeGraph createTimeGraph(){
        TimeGraph timeGraph = new TimeGraph();
        timeGraph.addVertex("start");
        timeGraph.addVertex("pre-middle");
        timeGraph.addVertex("suf-middle");
        timeGraph.addVertex("end");
        timeGraph.addEdge("pre-middle", "start", 8000, 0);
        timeGraph.addEdge("suf-middle", "start", 9000, 0);
        timeGraph.addEdge("suf-middle", "pre-middle", 5000, 2000);
        timeGraph.addEdge("end", "start", 17000, 6000);
        timeGraph.addEdge("end", "pre-middle", 8000, 4000);
        timeGraph.addEdge("end", "suf-middle", 7000, 1000);
        return timeGraph;
    }

    public static boolean timeMatch(String tailName, TimeGraph timeGraph, Event value,
                                    IterativeCondition.Context<Event> ctx) throws Exception{
        boolean flag = true;
        List<Edge> EdgeList = timeGraph.targetEdges(tailName);
        Map<String, Long> vertexTime = new HashMap<>();
        for (Edge current : EdgeList) {
            List<Event> contextEvents = Lists.newArrayList(ctx.getEventsForPattern(current.getHeadName()));
            if (contextEvents.size() > 0) {
                Event first = contextEvents.get(0);
                long interval  = (long) value.getAttribute("startTimeStamp").get() -
                        (long) first.getAttribute("startTimeStamp").get();
                vertexTime.put(current.getHeadName(), interval);
            }
        }

        for (Edge current : EdgeList) {
            long interval  =  vertexTime.get(current.getHeadName());
            if (!(interval < current.getUpperLimit() && interval > current.getLowerLimit())) {
                    flag = false;
            }
        }
        /*for (Edge current : EdgeList) {
            List<Event> contextEvents = Lists.newArrayList(ctx.getEventsForPattern(current.getHeadName()));
            if (contextEvents.size() > 0) {
                Event first = contextEvents.get(0);
                long interval = (long) value.getAttribute("startTimeStamp").get() -
                        (long) first.getAttribute("startTimeStamp").get();
                if (!(interval < current.getUpperLimit() && interval > current.getLowerLimit())) {
                    return false;
                }
            }
        }*/
        return flag;
    }

    public static void main(String[] args) throws Exception {

        BufferedWriter out = new BufferedWriter(new FileWriter("/root/env1/graph-result.txt"));
        int i = 0;
        while(i<100) {
            long processStart = System.currentTimeMillis();
            StreamExecutionEnvironment FileEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            FileEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            FileEnv.setParallelism(1);

            DataStream<Event> eventDataStream = FileEnv.addSource(new FileEventSource());

            Pattern<Event, Event> MyPattern = Pattern.<Event>begin("start").where(new IterativeCondition<Event>() {
                @Override
                public boolean filter(Event value, Context<Event> ctx) throws Exception {
                    if (value.getAttribute("eventType").isPresent() && value.getAttribute("eventType").get().equals("A")) {
                        return value.getAttribute("value").isPresent() && (int) value.getAttribute("value").get() > 80;
                    }
                    return false;
                }
            }).followedBy("pre-middle").where(new IterativeCondition<Event>() {
                @Override
                public boolean filter(Event value, Context<Event> ctx) throws Exception {
                    if (value.getAttribute("eventType").isPresent() && value.getAttribute("eventType").get().equals("B")) {
                        if(GraphCep.timeMatch("pre-middle", timeGraph, value, ctx)){
                            return value.getAttribute("value").isPresent() && (int) value.getAttribute("value").get() > 60;
                        }
                    }
                    return false;
                }
            }).followedBy("suf-middle").where(new IterativeCondition<Event>() {
                @Override
                public boolean filter(Event value, Context<Event> ctx) throws Exception {
                    if (value.getAttribute("eventType").isPresent() && value.getAttribute("eventType").get().equals("C")) {
                        if(GraphCep.timeMatch("suf-middle", timeGraph, value, ctx)){
                            return value.getAttribute("value").isPresent() && (int) value.getAttribute("value").get() > 60;
                        }
                    }
                    return false;
                }
            }).followedBy("end").where(new IterativeCondition<Event>() {
                @Override
                public boolean filter(Event value, Context<Event> ctx) throws Exception {
                    if (value.getAttribute("eventType").isPresent() && value.getAttribute("eventType").get().equals("D")) {
                        if(GraphCep.timeMatch("end", timeGraph, value, ctx)){
                            return value.getAttribute("value").isPresent() && (int) value.getAttribute("value").get() > 20;
                        }
                    }
                    return false;
                }
            }).within(Time.seconds(15));

            PatternStream<Event> patternStream = CEP.pattern(eventDataStream, MyPattern);


            OutputTag<String> outputTag = new OutputTag<String>("myOutput") {
            };

            SingleOutputStreamOperator<String> resultStream = patternStream.select(outputTag,
                    new PatternTimeoutFunction<Event, String>() {
                        @Override
                        public String timeout(Map<String, List<Event>> pattern, long timeoutTimestamp) throws Exception {
                            StringBuilder timeoutResult = new StringBuilder();
                            for (List<Event> eventList : pattern.values()) {
                                for (Event event : eventList) {
                                    timeoutResult.append(event.toString());
                                }
                            }
                            return "timeout Pattern:" + timeoutResult.toString();
                        }
                    }, new PatternSelectFunction<Event, String>() {
                        @Override
                        public String select(Map<String, List<Event>> map) throws Exception {
                            StringBuilder result = new StringBuilder();
                            for (List<Event> eventList : map.values()) {
                                for (Event event : eventList) {
                                    result.append(event.toString());
                                }
                            }
                            return "Matched Pattern" + result.toString();
                        }
                    });

            //resultStream.print();

            DataStream<String> sideOutput = resultStream.getSideOutput(outputTag);
            //sideOutput.print();

            System.out.println("Pattern Match Begin");
            System.out.println("This is " + (i + 1) + " times job");
            JobExecutionResult result = FileEnv.execute("My Test DslCEP");
            out.write(Long.toString(result.getNetRuntime(TimeUnit.MILLISECONDS)) + "\n");
            long processEnd = System.currentTimeMillis();
            System.out.println("ProcessTime:" + (processEnd - processStart));
            i++;
        }
        out.write("complete!");
        out.close();
    }
}
