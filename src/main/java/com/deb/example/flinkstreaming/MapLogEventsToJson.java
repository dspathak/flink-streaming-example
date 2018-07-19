package com.deb.example.flinkstreaming;

import org.apache.flink.api.common.functions.MapFunction;

public class MapLogEventsToJson implements MapFunction<LogEvent, String> {

	public String map(LogEvent le) throws Exception {
    	String leJson = le.toJson();
        return leJson;
    }
}
