package com.deb.example.flinkstreaming;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
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
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DebAppV3 {

	private static String FILEPATH = "/work/projects/expt/data/flinkstaging/";
	//private static String FILEPATH = "file:///work/projects/expt/data/flinkstaging/";
	//private static String FILEPATH = "hdfs://adc01dys.us.oracle.com:9000/user/dspathak/flinkdata/staging";
	private static int TIMEWINDOW = 60; //in secs

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(1);

		if(args != null) {
			try { int p = Integer.parseInt(args[0]); if (p>0) { env.setParallelism(p); }
			} catch (Exception e) { /*Ignore*/ }			
		}	

		TextInputFormat format = new TextInputFormat(new org.apache.flink.core.fs.Path(FILEPATH));
		format.setFilesFilter(FilePathFilter.createDefaultFilter());

		DataStream<LogEvent> inputStream = env
				.readFile(format, FILEPATH, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
				.map(new MapJsonToLogEvents())
				.filter(new FilterFunction<LogEvent>() {
					public boolean filter(LogEvent logEvent) {
						if (logEvent.getEventCategory().equalsIgnoreCase("login")) { return true; }	
						return false;
					}
				})
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LogEvent>(Time.milliseconds(10)) {
					public long extractTimestamp(LogEvent element) {
						return element.getTimeLong();
					}
				})
				;

		inputStream.print();

		DataStream<String> outputStream = inputStream				
				.keyBy(new KeySelector<LogEvent, String>() {
					public String getKey(LogEvent le) throws Exception {
						return le.getUser()+":"+le.getMachine();
					}
				})
				.timeWindow(Time.seconds(15))
                .apply(new WindowFunction<LogEvent, String, String, TimeWindow>() {
                    //@Override
                    public void apply(String key, TimeWindow window, Iterable<LogEvent> input, Collector<String> out) throws Exception {
                        long count = 0;
                        String s = key;
                        for (LogEvent le : input) {
                            count++;
                            s = s+":"+le.getRecordType();
                        }
                        out.collect(s+"-"+count+"-"+(new Date(window.getStart())).toString()+"-"+(new Date(window.getEnd())).toString());
                    }
                });
				//.process(new MyProcessWindowFunction());
		
		outputStream.print();


		/*

		Pattern<LogEvent, ?> mflPattern = Pattern.<LogEvent> begin("mfl")
				.subtype(LogEvent.class).where(
					new SimpleCondition<LogEvent>() {
						public boolean filter(LogEvent logEvent) {
							if (logEvent.getResult().equalsIgnoreCase("failed")) { return true; }
							return false;
						}
					})
				.timesOrMore(5)//.greedy()			
				.within(Time.seconds(TIMEWINDOW));

		PatternStream<LogEvent> mflPatternStream = CEP.pattern(inputLogEventStream, mflPattern);

		DataStream<Threat> outputMflStream = mflPatternStream.select(
				new PatternSelectFunction<LogEvent, Threat>() {
					public Threat select(Map<String, List<LogEvent>> logEventsMap) throws Exception {
						Threat a = new Threat("");
						String m = "MULTIPLE FAILED LOGINS detected!";
						for (Map.Entry<String, List<LogEvent>> entry : logEventsMap.entrySet()) {
							m = m + "\n" + entry.getKey() + toShortStringLogEvents(entry.getValue());
						}
						a.setMessage(m);
						return a;
					}
				});

		outputMflStream.print();

		Pattern<LogEvent, ?> bfaPattern = mflPattern.followedBy("bfa")
				.subtype(LogEvent.class).where(
					new SimpleCondition<LogEvent>() {
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
