package com.deb.example.flinkstreaming;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class DebAppCEPKafka_Bkup1 {
	
	public static void main(String[] args) throws Exception {
		
		int TIMEWINDOW_SECS = 60;
		int MAX_TOLERABLE_EVENT_DELAY_SECS = 10;
		int WATERMARK_GENERATION_FREQUENCY_MILLIS = 1000;
		int FAILED_LOGIN_COUNT = 5;
		int SUCCESS_COUNT = 1;		
		
		/*
		e.g.  --input hdfs:///mydata --elements 42 from the command line
		
		--tenantIds tenant1 tenant2 tenant3
		--consumerGroup kafkaSMA_tenant123
		--brokerList localhost:9092
		--inputTopic xyz
		--outputTopic abc
		--envParallelism 2
		--taskParallelism 4
		
		--timeWindow
		--successCount
		--failedLoginCount
		--maxTolerableEventDelay
		
		String propertiesFile = "/home/sam/flink/myjob.properties";
		ParameterTool parameters = ParameterTool.fromPropertiesFile(propertiesFile);
		OR		
		ParameterTool parameters = ParameterTool.fromArgs(args);		
		*/		
		
		// set up the execution environment
		Configuration conf = new Configuration();
		conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
		final StreamExecutionEnvironment env = LocalStreamEnvironment.createLocalEnvironment(1, conf);		
		//final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(WATERMARK_GENERATION_FREQUENCY_MILLIS);
		
		ParameterTool parameters = ParameterTool.fromArgs(args);
		final List<String> TENANTIDS = parseTenantIds(parameters.get("tenantIds", ""));
		String CONSUMER_GROUP = parameters.get("consumerGroup");
		String BROKERLIST = parameters.get("brokerlist", "localhost:9092");
		String FROM_TOPIC = parameters.get("inputTopic", "flink2PartitionsCEPNew");
		String TO_TOPIC = parameters.get("outputTopic", "flink2PartitionsCEPOutput");				
		int ENV_PARALLELISM = parameters.getInt("envParallelism",2);
		int TASK_PARALLELISM = parameters.getInt("taskParallelism",4);
		
		if (ENV_PARALLELISM > 0) { env.setParallelism(ENV_PARALLELISM); }
		if (TASK_PARALLELISM <= 0) { TASK_PARALLELISM = 4; }	

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers",BROKERLIST);
		properties.setProperty("group.id", CONSUMER_GROUP);
		DataStream<String> kafkaMessageStream = env.addSource(new FlinkKafkaConsumer010<String> (FROM_TOPIC, new SimpleStringSchema(), properties));

		DataStream<LogEvent> inputStream = kafkaMessageStream
				.map(new MapJsonToLogEvents()).setParallelism(TASK_PARALLELISM)
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LogEvent>(Time.seconds(MAX_TOLERABLE_EVENT_DELAY_SECS)) {
					public long extractTimestamp(LogEvent element) {
			            return element.getTimeLong();
			        }
				}).setParallelism(TASK_PARALLELISM)
				.filter(new FilterFunction<LogEvent>() {
					public boolean filter(LogEvent logEvent) {
						if (TENANTIDS.contains(logEvent.getTenantId()) && logEvent.getEventCategory().equals("login")) { return true; }	
						return false;
					}
				}).setParallelism(TASK_PARALLELISM)
				.keyBy(new KeySelector<LogEvent, String>() {
					public String getKey(LogEvent le) throws Exception {
		                return le.getTenantId()+":"+le.getUser()+":"+le.getMachine();
		            }
		        });
		
		//inputStream.print();		

		Pattern<LogEvent, ?> mflPattern = Pattern.<LogEvent> begin("mfl")
				.subtype(LogEvent.class).where(
					new SimpleCondition<LogEvent>() {
						public boolean filter(LogEvent logEvent) {
							if (logEvent.getResult().equals("failed")) { return true; }
							return false;
						}
					})
				//.timesOrMore(FAILED_LOGIN_COUNT)
				.times(FAILED_LOGIN_COUNT)
				.within(Time.seconds(TIMEWINDOW_SECS));
		
		PatternStream<LogEvent> mflPatternStream = CEP.pattern(inputStream, mflPattern);
		
		/*
		DataStream<Threat> outputMflStream = mflPatternStream.select(
				new PatternSelectFunction<LogEvent, Threat>() {
					public Threat select(Map<String, List<LogEvent>> logEventsMap) throws Exception {
						Threat a = new Threat("");
						//String m = "MULTIPLE FAILED LOGINS detected!";
						String m = "**MFL**";
						for (Map.Entry<String, List<LogEvent>> entry : logEventsMap.entrySet()) {
							m = m + entry.getKey() + toShortStringLogEvents(entry.getValue());
						}
						a.setMessage(m);
						return a;
					}
				}).setParallelism(4);
		*/
		
		DataStream<String> outputMflStream = mflPatternStream.select(
				new PatternSelectFunction<LogEvent, String>() {
					public String select(Map<String, List<LogEvent>> logEventsMap) throws Exception {
						String m = "**MFL**";
						for (Map.Entry<String, List<LogEvent>> entry : logEventsMap.entrySet()) {
							m = m + entry.getKey() + toShortStringLogEvents(entry.getValue());
							//System.out.println("blah blah");
						}
						return m;
					}
				}).setParallelism(TASK_PARALLELISM);

		//outputMflStream.print();
		
		FlinkKafkaProducer010.FlinkKafkaProducer010Configuration mflProducerConfig = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
				outputMflStream, TO_TOPIC, new KeyedSerializationSchemaWrapper(new SimpleStringSchema()), properties);
		// the following is necessary for at-least-once delivery guarantee
		mflProducerConfig.setLogFailuresOnly(false); mflProducerConfig.setFlushOnCheckpoint(true); mflProducerConfig.setWriteTimestampToKafka(true);
		
		/* 
		 * What I was trying to do here is - take the MFL stream and then do a 'followedBy' success login to get BFA. IT WORKED !
		 * But I think we can do it another way too - Create MFL stream & Also create a 'Successful Login' stream. Connect the 2 streams & use RichCoFlatMapFunction
		 * Creating MFL, SuccesfulLogin streams is useful in case they need to be reused for other patterns
		 */
		Pattern<LogEvent, ?> bfaPattern = mflPattern.followedBy("bfa")
				.subtype(LogEvent.class).where(
					new SimpleCondition<LogEvent>() {
						public boolean filter(LogEvent logEvent) {
							if (logEvent.getResult().equals("success")) { return true; }
							return false;
						}
					})
				//.timesOrMore(SUCCESS_COUNT)
				.times(SUCCESS_COUNT)
				.within(Time.seconds(TIMEWINDOW_SECS));
		
		PatternStream<LogEvent> bfaPatternStream = CEP.pattern(inputStream, bfaPattern);
		
		/*
		DataStream<Threat> outputBfaStream = bfaPatternStream.select(
				new PatternSelectFunction<LogEvent, Threat>() {
					public Threat select(Map<String, List<LogEvent>> logEventsMap) throws Exception {
						Threat a = new Threat("");
						//String m = "BRUTE FORCE ATTACK detected!";
						String m = "**BFA**";
						for (Map.Entry<String, List<LogEvent>> entry : logEventsMap.entrySet()) {
							m = m + entry.getKey() + toShortStringLogEvents(entry.getValue());
						}
						a.setMessage(m);
						return a;
					}
				}).setParallelism(TASK_PARALLELISM);
		*/
		
		DataStream<String> outputBfaStream = bfaPatternStream.select(
				new PatternSelectFunction<LogEvent, String>() {
					public String select(Map<String, List<LogEvent>> logEventsMap) throws Exception {
						String m = "**BFA**";
						for (Map.Entry<String, List<LogEvent>> entry : logEventsMap.entrySet()) {
							m = m + entry.getKey() + toShortStringLogEvents(entry.getValue());
						}
						return m;
					}
				}).setParallelism(TASK_PARALLELISM);
		
		//outputBfaStream.print();
		
		FlinkKafkaProducer010.FlinkKafkaProducer010Configuration bfaProducerConfig = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
				outputBfaStream, TO_TOPIC, new KeyedSerializationSchemaWrapper(new SimpleStringSchema()), properties);
		// the following is necessary for at-least-once delivery guarantee
		bfaProducerConfig.setLogFailuresOnly(false); bfaProducerConfig.setFlushOnCheckpoint(true); bfaProducerConfig.setWriteTimestampToKafka(true);
		
		// execute program
		env.execute("CEP Threat Detection - "+CONSUMER_GROUP);
	}
	
	private static String toShortStringLogEvents(List<LogEvent> les) {
		String r = "";
		for (LogEvent le : les) {
			r = r + "|::|" + le.toShortString();
		}
		return r;
	}
	
	private static List<String> parseTenantIds(String unparsedTenantIds) {
		String[] tIds = unparsedTenantIds.split(",");
		List<String> r = new ArrayList<String>(tIds.length);
		for (int i=0; i<tIds.length; i++) {
			r.add(tIds[i].trim());
		}
		return r;
	}
}
