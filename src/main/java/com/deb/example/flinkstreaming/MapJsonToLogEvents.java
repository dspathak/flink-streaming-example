package com.deb.example.flinkstreaming;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.flink.api.common.functions.MapFunction;
import org.codehaus.jackson.map.ObjectMapper;

public class MapJsonToLogEvents implements MapFunction<String, LogEvent> {

	static private final ObjectMapper mapper = new ObjectMapper();

    public LogEvent map(String eventJson) throws Exception {    	
        LogEvent logEvent = mapper.readValue(eventJson, LogEvent.class);
        logEvent.setTimeLong(LogEvent.convertToLong(logEvent.getTime()));
        //Thread.sleep(5);
        return logEvent;
    }
    /*
    private static Long convertToLong(String timestamp) {
//		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneId.of("UTC"));
//		return ZonedDateTime.parse(timestamp,formatter).toEpochSecond();
    	DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
		long millisSinceEpoch = LocalDateTime.parse(timestamp, formatter).atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
		return millisSinceEpoch;
	}
	*/
}
