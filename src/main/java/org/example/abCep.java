package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.cep.PatternStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.List;
import java.util.Map;


public class abCep {
    public static void main(String[] args)throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<Tuple2<String,String>> MyDataStream = env.addSource(new abSource()).map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                JSONObject json = JSON.parseObject(s);
                return new Tuple2<>(json.getString("eventType"), json.getString("timeStamp"));
            }
        });

        Pattern<Tuple2<String,String>,Tuple2<String,String>> MyPattern = Pattern.<Tuple2<String,String>>begin("start").where(
                new IterativeCondition<Tuple2<String, String>>() {
                    @Override
                    public boolean filter(Tuple2<String, String> value, Context<Tuple2<String, String>> context) throws Exception {
                        return value.f0.equals("A");
                    }
                }
        ).followedBy("next").where(new IterativeCondition<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value, Context<Tuple2<String, String>> cxt) throws Exception {
                long now = Long.parseLong(value.f1);
                long count = now - cxt.timestamp();
                if(value.f0.equals("B") && count < 2000)
                    return true;
                return false;
            }
        }).followedBy("end").where(new SimpleCondition<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) throws Exception {
                return value.f0.equals("C");
            }
        }).within(Time.seconds(6));

        PatternStream<Tuple2<String, String>> pattern = CEP.pattern(MyDataStream, MyPattern);

        OutputTag<String> outputTag = new OutputTag<String>("myOutput"){};

        SingleOutputStreamOperator<String> resultStream = pattern.select(outputTag,
                new PatternTimeoutFunction<Tuple2<String, String>, String>() {
                    @Override
                    public String timeout(Map<String, List<Tuple2<String, String>>> pattern, long timeoutTimestamp) throws Exception {
                        //List<Tuple3<String, String, String>> startList = pattern.get("start");
                        //Tuple3<String, String, String> tuple2 = startList.get(0);
                        return "timeout pattern:" + pattern;
                    }
                }, new PatternSelectFunction<Tuple2<String, String>, String>() {
                    @Override
                    public String select(Map<String, List<Tuple2<String, String>>> pattern) throws Exception {
                        //List<Tuple3<String,String,String>> startList = pattern.get("start");
                        //List<Tuple3<String,String,String>> endList = pattern.get("next");
                        //Tuple3<String,String,String> startTuple2 = startList.get(0);
                        //Tuple3<String,String,String> endTuple2 = endList.get(0);

                        //return startTuple3.toString() + endTuple3.toString();
                        return "matched pattern:" + pattern;

                    }
                });
        resultStream.print();

        DataStream<String> sideOutput = resultStream.getSideOutput(outputTag);
        sideOutput.print();

        env.execute("My Test abCEP");

    }
}
