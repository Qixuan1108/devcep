package org.example;

import at.datasciencelabs.pattern.Event;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Optional;

public class DslEvent implements Event {
    final private long startTimeStamp;
    final private long endTimeStamp;
    final private String eventType;
    final private int value;
    final private int eventID;

    DslEvent(String eventType, int value, int eventID){
        this.eventType = eventType;
        this.value = value;
        this.startTimeStamp = System.currentTimeMillis();
        this.endTimeStamp = this.startTimeStamp;
        this.eventID = eventID;
    }

    DslEvent(String[] eventValue){
        this.eventID = Integer.parseInt(eventValue[0]);
        this.eventType = eventValue[1];
        this.value = Integer.parseInt(eventValue[2]);
        this.startTimeStamp = Long.parseLong(eventValue[3]);
        this.endTimeStamp = Long.parseLong(eventValue[4]);
    }

    @Override
    public Optional<Object> getAttribute(String s) {
        Optional<Object> Attribute = Optional.empty();
        try {
            Field F1 = this.getClass().getDeclaredField(s);
            Attribute = Optional.ofNullable(F1.get(this));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return Attribute;
    }

    @Override
    public String getEventType() {
        return this.eventType;
    }

    public long getEndTimeStamp(){
        return this.endTimeStamp;
    }

    public long getStartTimeStamp(){
        return this.startTimeStamp;
    }

    public int getValue(){
        return this.value;
    }

    public int getEventID(){
        return this.eventID;
    }

    @Override
    public String toString() {
        SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String startTime = dateformat.format(this.startTimeStamp);
        String endTime = dateformat.format(this.endTimeStamp);

        return "Event(ID:"+ this.eventID + ", EventType:" + this.eventType + ", Value:" + this.value + ", StartTime:"
                + startTime + ", EndTime:" + endTime + ")";
    }
}
