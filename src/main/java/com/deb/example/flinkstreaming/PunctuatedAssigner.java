package com.deb.example.flinkstreaming;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;

public class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<LogEvent> {

	//@Override
	public long extractTimestamp(LogEvent element, long previousElementTimestamp) {
		return element.getTimeLong();
	}

	//@Override
	public Watermark checkAndGetNextWatermark(LogEvent lastElement, long extractedTimestamp) {
		long watermark = extractedTimestamp - 5000;
		System.out.println("Watermark  "+watermark);
		return new Watermark(watermark);
	}
}

