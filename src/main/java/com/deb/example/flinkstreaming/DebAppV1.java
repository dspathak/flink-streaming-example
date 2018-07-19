package com.deb.example.flinkstreaming;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class DebAppV1 {
	
	private static String FILEPATH = "/work/projects/expt/data/flinkstaging/";
	//private static String FILEPATH = "file:///work/projects/expt/data/flinkstaging/";
	//private static String FILEPATH = "hdfs://adc01dys.us.oracle.com:9000/user/dspathak/flinkdata/staging";
	private static int TIMEWINDOW = 60; //in secs
	
	public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(1000);
		
		if(args != null) {
			try { int p = Integer.parseInt(args[0]); if (p>0) { env.setParallelism(p); }
			} catch (Exception e) { /*Ignore*/ }			
		}	

		TextInputFormat format = new TextInputFormat(new org.apache.flink.core.fs.Path(FILEPATH));
		format.setFilesFilter(FilePathFilter.createDefaultFilter());

		DataStream<LogEvent> inputLogEventStream = env
				.readFile(format, FILEPATH, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
				.map(new MapJsonToLogEvents())				
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LogEvent>(Time.seconds(0)) {
					public long extractTimestamp(LogEvent element) {
			            return element.getTimeLong();
			        }
				})
				.keyBy(new KeySelector<LogEvent, String>() {
					public String getKey(LogEvent le) throws Exception {
		                return le.getUser();
		            }
		        });
		
		inputLogEventStream.print();

		Pattern<LogEvent, ?> mflPattern = Pattern.<LogEvent> begin("mfl")
				.subtype(LogEvent.class).where(
					new SimpleCondition<LogEvent>() {
						public boolean filter(LogEvent logEvent) {
							if (logEvent.getResult().equalsIgnoreCase("failed")) { return true; }
							return false;
						}
					})
				.timesOrMore(3).within(Time.seconds(TIMEWINDOW));
		
		PatternStream<LogEvent> mflPatternStream = CEP.pattern(inputLogEventStream, mflPattern);
				
		DataStream<Threat> outputMflStream = mflPatternStream.select(
				new PatternSelectFunction<LogEvent, Threat>() {
					public Threat select(Map<String, List<LogEvent>> logEventsMap) {
						return new Threat("MULTIPLE FAILED LOGINS detected!");
					}
				});

		outputMflStream.print();
		
		/* 
		 * What I was trying to do here is - take the MFL stream and then do a 'followedBy' success login to get BFA. IT WORKED !
		 * But I think we can do it another way too - Create MFL stream & Also create a 'Successful Login' stream. Connect the 2 streams & use RichCoFlatMapFunction
		 * Creating MFL, SuccesfulLogin streams is useful in case they need to be reused for other patterns
		 */
		/*
		Pattern<LogEvent, ?> bfaPattern = mflPattern.followedBy("bfa")
				.subtype(LogEvent.class).where(
					new SimpleCondition<LogEvent>() {
						private static final long serialVersionUID = 1L;
						public boolean filter(LogEvent logEvent) {
							if (logEvent.getResult().equalsIgnoreCase("success")) { return true; }
							return false;
						}
					})
				.timesOrMore(1)
				.within(Time.seconds(TIMEWINDOW));
		
		PatternStream<LogEvent> bfaPatternStream = CEP.pattern(inputLogEventStream, bfaPattern);
		
		DataStream<Threat> outputBfaStream = bfaPatternStream.select(
				new PatternSelectFunction<LogEvent, Threat>() {
					private static final long serialVersionUID = 1L;
					public Threat select(Map<String, List<LogEvent>> logEventsMap) throws Exception {
						Threat a = new Threat("");
						String m = "BRUTE FORCE ATTACK detected!";
						for (Map.Entry<String, List<LogEvent>> entry : logEventsMap.entrySet()) {
							m = m + "\n" + entry.getKey() + toShortStringLogEvents(entry.getValue());
						}
						a.setMessage(m);
						return a;
					}
				});
		
		outputBfaStream.print();
		*/
		
		// execute program
		env.execute("CEP Threat Detection");
	}
	
	private static String toShortStringLogEvents(List<LogEvent> les) {
		String r = "";
		for (LogEvent le : les) {
			r = r + "|::|" + le.toShortString();
		}
		return r;
	}	
}
