package org.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import at.datasciencelabs.pattern.Event;

import java.io.BufferedReader;
import java.io.FileReader;

public class FileEventSource extends RichSourceFunction<Event> {
    BufferedReader in;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.in = new BufferedReader(new FileReader("/root/env1/date_study.txt"));
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.in.close();
    }

    @Override
    public void run(SourceContext<Event> sourceContext){
        DslEvent eventBuild;
        String eventString;
        try {
            while ((eventString = this.in.readLine()) != null) {
                eventBuild = new DslEvent(eventString.split(","));
                long timeStamp = eventBuild.getStartTimeStamp();
                sourceContext.collectWithTimestamp(eventBuild, timeStamp);
                sourceContext.emitWatermark(new Watermark(timeStamp - 5000));
            }
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
    }



    @Override
    public void cancel() {

    }
}
