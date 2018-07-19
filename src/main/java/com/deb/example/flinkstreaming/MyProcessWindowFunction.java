package com.deb.example.flinkstreaming;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyProcessWindowFunction extends ProcessWindowFunction<LogEvent, String, String, TimeWindow> {

	public void process(String key, Context context, Iterable<LogEvent> input, Collector<String> out) {
		long count = 0;
		for (LogEvent in: input) {
			count++;
		}
		out.collect("Window: " + context.window() + "count: " + count);
	}
}
