package com.deb.example.flinkstreaming;

import java.util.Date;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DebAppV4 {
	
	private static String FILEPATH = "/work/projects/expt/data/flinkstaging/";
	//private static String FILEPATH = "file:///work/projects/expt/data/flinkstaging/";
	//private static String FILEPATH = "hdfs://adc01dys.us.oracle.com:9000/user/dspathak/flinkdata/staging";
	private static int TIMEWINDOW = 15; //in secs
	
	public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(8);
		
		if(args != null) {
			try { int p = Integer.parseInt(args[0]); if (p>0) { env.setParallelism(p); }
			} catch (Exception e) { /*Ignore*/ }			
		}	

		TextInputFormat format = new TextInputFormat(new org.apache.flink.core.fs.Path(FILEPATH));
		format.setFilesFilter(FilePathFilter.createDefaultFilter());
		
		/*

		DataStream<LogEvent> inputLogEventStream = env
				.readFile(format, FILEPATH, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
				.map(new MapToLogEvents())				
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
		
		*/
		
		//////////////////////
		
		DataStream<LogEvent> inputStream = env
				.readFile(format, FILEPATH, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000)
				.map(new MapJsonToLogEvents())
//				.assignTimestampsAndWatermarks(new PunctuatedAssigner())
				.filter(new FilterFunction<LogEvent>() {
					public boolean filter(LogEvent logEvent) {
						if (logEvent.getEventCategory().equalsIgnoreCase("login")) { return true; }	
						return false;
					}
				})
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LogEvent>(Time.seconds(0)) {
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
				.timeWindow(Time.seconds(TIMEWINDOW))
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
		
		outputStream.print();
		
		// execute program
		env.execute("Threat Detection");
	}
}
