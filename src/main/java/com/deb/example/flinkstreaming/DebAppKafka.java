package com.deb.example.flinkstreaming;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Queue;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

public class DebAppKafka {

	private static int TIMEWINDOW = 60; //in secs
	private static int MAX_TOLERABLE_EVENT_DELAY = 10; //in secs
	private static int FAILED_LOGIN_COUNT = 5;
	private static int SUCCESS_COUNT = 1;

	private static String TOPIC = "flink2PartitionsWINDOWED";
	private static String CONSUMER_GROUP = "flinkconsumerWINDOWED";
	private static String BROKERLIST = "localhost:9092";

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(1000); //millisecs

		if(args != null) {
			try { int p = Integer.parseInt(args[0]); if (p>0) { env.setParallelism(p); }
			} catch (Exception e) { /*Ignore*/ }			
		}

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers",BROKERLIST);
		properties.setProperty("group.id", CONSUMER_GROUP);
		DataStream<String> kafkaMessageStream = env.addSource(new FlinkKafkaConsumer010<String> (TOPIC, new SimpleStringSchema(), properties));

		/*
		DataStream<LogEvent> inputStream = kafkaMessageStream
				.map(new MapToLogEvents())
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LogEvent>(Time.seconds(MAX_TOLERABLE_EVENT_DELAY)) {
					public long extractTimestamp(LogEvent element) {
						return element.getTimeLong();
					}
				})
				.filter(new FilterFunction<LogEvent>() {
					public boolean filter(LogEvent logEvent) {
						if (logEvent.getEventCategory().equalsIgnoreCase("login")) { return true; }	
						return false;
					}
				});

		inputStream.print();
		 */

		KeyedStream<LogEvent,String> inputStream = kafkaMessageStream
				.map(new MapJsonToLogEvents())
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LogEvent>(Time.seconds(MAX_TOLERABLE_EVENT_DELAY)) {
					public long extractTimestamp(LogEvent element) {
						return element.getTimeLong();
					}
				})
				.filter(new FilterFunction<LogEvent>() {
					public boolean filter(LogEvent logEvent) {
						if (logEvent.getEventCategory().equalsIgnoreCase("login")) { return true; }	
						return false;
					}
				})
				.keyBy(new KeySelector<LogEvent, String>() {
					public String getKey(LogEvent le) throws Exception {
						return le.getUser()+":"+le.getMachine();
					}
				});

		inputStream.print();

		DataStream<String> outputStream = inputStream				
				//				.keyBy(new KeySelector<LogEvent, String>() {
				//					public String getKey(LogEvent le) throws Exception {
				//						return le.getUser()+":"+le.getMachine();
				//					}
				//				})
				.timeWindow(Time.seconds(TIMEWINDOW))
				.apply(new WindowFunction<LogEvent, String, String, TimeWindow>() {
					//@Override
					public void apply(String key, TimeWindow window, Iterable<LogEvent> input, Collector<String> out) throws Exception {
						long count = 0;
						long count1 = 0;
						String s = key;
						String s1 = key;

						Queue<LogEvent> logEventPriorityQueue = new PriorityQueue<LogEvent>(15, logEventComparator);
						Queue<LogEvent> logEventPriorityQueue1 = new PriorityQueue<LogEvent>(15, logEventComparator);
						for (LogEvent le : input) {
							//logEventPriorityQueue.add(le);
							logEventPriorityQueue.offer(le);
							logEventPriorityQueue1.offer(le);
						}

						for (LogEvent le : input) {
							count++;
							s = s+":"+le.getRecordType();
						}

						while(true){
							LogEvent le = logEventPriorityQueue.poll();
							if(le == null) break;
							count1++;
							s1 = s1+":"+le.getRecordType();
						}

						String s2o = detectThreat(logEventPriorityQueue1);

						String so = s+"-"+count+"-"+(new Date(window.getStart())).toString()+"-"+(new Date(window.getEnd())).toString();
						String s1o = s1+"-"+count1+"-"+(new Date(window.getStart())).toString()+"-"+(new Date(window.getEnd())).toString();

						out.collect(so + " || " + s1o + " || " + s2o);
					}
				});

		/*
		 * For computing MFL & BFA, first get the Iterable input into a Priority Queue. That will order them by event time.
		 * Then loops thru this to find X consequitive failures followed by a success.
		 */

		outputStream.print();

		// execute program
		env.execute("Windowed Threat Detection");
	}

	//Comparator anonymous class implementation
	public static Comparator<LogEvent> logEventComparator = new Comparator<LogEvent>(){
		//@Override
		public int compare(LogEvent le1, LogEvent le2) {
			int r = (int) (le1.getTimeLong() - le2.getTimeLong());
			if (r == 0) {
				if ("success".equals(le1.getResult()) && "failed".equals(le2.getResult())) {
					r = 1;
				} else if ("failed".equals(le1.getResult()) && "success".equals(le2.getResult())) {
					r = -1;
				}
			}
			return r;
		}
	};

	public static String detectThreat(Queue<LogEvent> logEventPriorityQueue) {		
		int fl = 0;
		int sl = 0;
		List<LogEvent> tempList = new ArrayList<LogEvent>();
		String r = "";
		String r1 = "";

		while(true){
			LogEvent le = logEventPriorityQueue.poll();
			if(le == null) {
				break;
			} else if ("failed".equals(le.getResult())) {				
				fl++;
				tempList.add(le);
				if (fl >= FAILED_LOGIN_COUNT) {
					//System.out.println("MFL");
					r = r+"MFL  ";
					printMFL(tempList);
				} /*else if (fl == FAILED_LOGIN_COUNT+1) {
					fl = 1;
					tempList.clear();
					tempList.add(le);
				}*/
			} else if ("success".equals(le.getResult())) {
				sl++;
				tempList.add(le);
				if (sl == SUCCESS_COUNT) { 
					if (fl >= FAILED_LOGIN_COUNT) {
						//System.out.println("BFA");
						r1 = r1+r+"BFA ";
						printBFA(tempList);
					}
					r = "";						
					tempList.clear();
					fl = 0;
					sl = 0;
				}
			} else {
				continue;
			}
		}
		return r1+r;
	}

	private static void printBFA(List<LogEvent> tempList) {
		// TODO Auto-generated method stub

	}

	private static void printMFL(List<LogEvent> tempList) {
		// TODO Auto-generated method stub

	}
}
