package org.example;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import at.datasciencelabs.pattern.Event;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class EventSource extends RichSourceFunction<Event> {
    static int eventID = 1;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        int randomNum;
        DslEvent eventBuild;
        try {
            while (eventID < 200) {
                randomNum = (int) (Math.random() * 100);
                if (randomNum < 50) {
                    eventBuild = new DslEvent("A", randomNum, eventID);
                } else {
                    eventBuild = new DslEvent("B", 610, eventID);
                }
                eventID++;
                long timeStamp = eventBuild.getStartTimeStamp();
                sourceContext.collectWithTimestamp(eventBuild, timeStamp);
                sourceContext.emitWatermark(new Watermark(timeStamp - 5000));
                Thread.sleep((int) (Math.random() * 3000));
            }
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    @Override
    public void cancel() {

    }
}
