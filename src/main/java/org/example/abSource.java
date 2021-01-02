package org.example;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.Map;

public class abSource extends RichSourceFunction<String> {
    static int EventId = 1000;
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        Map<String,String> map = new HashMap<>();
        String timeStampString;
        int randomNum;
        long timeStamp = System.currentTimeMillis();
        System.out.println("source begin to create event" + timeStamp);
        while(EventId < 2000){
            randomNum = (int)(Math.random() * 100);
            if(randomNum > 0 && randomNum <= 50){
                map.put("eventType", "A");
            }else if(randomNum > 50 && randomNum <= 80){
                map.put("eventType", "B");
            }else{
                map.put("eventType", "C");
            }
            timeStamp = System.currentTimeMillis();
            timeStampString = "" + timeStamp;
            map.put("timeStamp", timeStampString);
            sourceContext.collectWithTimestamp(JSON.toJSONString(map),timeStamp);
            sourceContext.emitWatermark(new Watermark(timeStamp-5000));
            EventId++;
            Thread.sleep((int)(Math.random() * 5000));
        }
    }

    @Override
    public void cancel() {

    }
}
