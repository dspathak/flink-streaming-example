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

public class DebAppCEPKafka_Alternate {
	
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
		
		/**********************************************************************************************************************
		* For local run from within IDE, if WEB UI is needed, please uncomment the below line & comment out the line after that		 
		**********************************************************************************************************************/		
		//final StreamExecutionEnvironment env = LocalStreamEnvironment.createLocalEnvironment(1, conf);		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(WATERMARK_GENERATION_FREQUENCY_MILLIS);
		env.enableCheckpointing(1000);
		
		ParameterTool parameters = ParameterTool.fromArgs(args);
		final List<String> TENANTIDS = parseTenantIds(parameters.get("tenantIds", ""));
		String CONSUMER_GROUP = parameters.get("consumerGroup");
		String BROKERLIST = parameters.get("brokerlist", "localhost:9092");
		String FROM_TOPIC = parameters.get("inputTopic", "flink2PartitionsCEPNew");
		String TO_TOPIC = parameters.get("outputTopic", "flink2PartitionsCEPOutputAlternative");				
		int ENV_PARALLELISM = parameters.getInt("envParallelism",2);
		int TASK_PARALLELISM = parameters.getInt("taskParallelism",4);
		
		if (ENV_PARALLELISM > 0) { env.setParallelism(ENV_PARALLELISM); }
		if (TASK_PARALLELISM <= 0) { TASK_PARALLELISM = 4; }	

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers",BROKERLIST);
		properties.setProperty("group.id", CONSUMER_GROUP);
		DataStream<String> kafkaMessageStream = env.addSource(new FlinkKafkaConsumer010<String> (FROM_TOPIC, new SimpleStringSchema(), properties));

		/***
		 *A*
		 ***/
		DataStream<LogEvent> inputStream = kafkaMessageStream
				.map(new MapJsonToLogEvents()).setParallelism(TASK_PARALLELISM)
				.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LogEvent>(Time.seconds(MAX_TOLERABLE_EVENT_DELAY_SECS)) {
					public long extractTimestamp(LogEvent element) {
			            return element.getTimeLong();
			        }
				}).setParallelism(TASK_PARALLELISM)
				.filter(new FilterFunction<LogEvent>() {
					public boolean filter(LogEvent logEvent) {
						if (TENANTIDS.contains(logEvent.getTenantId()) && "login".equals(logEvent.getEventCategory())) { return true; }	
						return false;
					}
				}).setParallelism(TASK_PARALLELISM);
		
		/***
		 *B*
		 ***/
		DataStream<LogEvent> failedLoginStream = inputStream
				.filter(new FilterFunction<LogEvent>() {
					public boolean filter(LogEvent logEvent) {
						if ("failed".equals(logEvent.getResult())) { return true; }
						return false;
					}
				}).setParallelism(TASK_PARALLELISM)
				.keyBy(new KeySelector<LogEvent, String>() {
					public String getKey(LogEvent le) throws Exception {
		                return le.getTenantId()+":"+le.getUser()+":"+le.getMachine();
		            }
		        });
		//failedLoginStream.print();
		
		/***
		 *C*
		 ***/
		DataStream<LogEvent> successfulLoginStream = inputStream
				.filter(new FilterFunction<LogEvent>() {
					public boolean filter(LogEvent logEvent) {
						if ("success".equals(logEvent.getResult())) { return true; }
						return false;
					}
				}).setParallelism(TASK_PARALLELISM);
		//successfulLoginStream.print();
		
		
		/****
		 *P1*  On stream B
		 ****/
		Pattern<LogEvent, ?> mflPattern = Pattern.<LogEvent> begin("FAILED_LOGINS")
				.times(FAILED_LOGIN_COUNT)
				.greedy()
				.within(Time.seconds(TIMEWINDOW_SECS));
		
		
		PatternStream<LogEvent> mflPatternStream = CEP.pattern(failedLoginStream, mflPattern);
		
		DataStream<LogEvent> outputMflStream = mflPatternStream.select(
				new PatternSelectFunction<LogEvent, LogEvent>() {
					public LogEvent select(Map<String, List<LogEvent>> logEventsMap) throws Exception {
						List<LogEvent> leList = new ArrayList<LogEvent>();
						for (Map.Entry<String, List<LogEvent>> entry : logEventsMap.entrySet()) {
							LogEvent le = extractMFLThreat(entry.getValue());
							leList.add(le);
						}
						if (leList.size() != 1) System.out.println("Something wrong while extracting MFL Threat !!");
						return leList.get(0);
					}
				}).setParallelism(TASK_PARALLELISM);
		//outputMflStream.print();
		
		
		DataStream<String> outputMflJsonStream = outputMflStream.map(new MapLogEventsToJson()).setParallelism(TASK_PARALLELISM);
		
		
		FlinkKafkaProducer010.FlinkKafkaProducer010Configuration mflProducerConfig = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
				outputMflJsonStream, TO_TOPIC, new KeyedSerializationSchemaWrapper(new SimpleStringSchema()), properties);
		// the following is necessary for at-least-once delivery guarantee
		mflProducerConfig.setLogFailuresOnly(false); mflProducerConfig.setFlushOnCheckpoint(true); mflProducerConfig.setWriteTimestampToKafka(true);
		
		/****
		 *P2*   On stream output of P1 + stream C
		 ****/
		
		//Merge output MFL Threat Streams & osef successful login stream
		DataStream<LogEvent> mflPlusSuccessfulLoginStream = outputMflStream.union(successfulLoginStream)
				.keyBy(new KeySelector<LogEvent, String>() {
					public String getKey(LogEvent le) throws Exception {
		                return le.getTenantId()+":"+le.getUser()+":"+le.getMachine();
		            }
		        });		
		//mflPlusSuccessfulLoginStream.print();
				
		Pattern<LogEvent, ?> bfaPattern = Pattern.<LogEvent> begin("MULTIPLE_FAILED_LOGINS")
				.where(
					new SimpleCondition<LogEvent>() {
						public boolean filter(LogEvent logEvent) {
							if (logEvent.getEventCategory().equals("MFL")) { return true; }
							return false;
						}
					})
				.oneOrMore()				
				.greedy()
				.followedBy("SUCCESSFUL_LOGIN")
				.where(
					new SimpleCondition<LogEvent>() {
						public boolean filter(LogEvent logEvent) {
							if ("success".equals(logEvent.getResult())) { return true; }
							return false;
						}
					})
				.within(Time.seconds(TIMEWINDOW_SECS));
		
		PatternStream<LogEvent> bfaPatternStream = CEP.pattern(mflPlusSuccessfulLoginStream, bfaPattern);
		
		DataStream<LogEvent> outputBfaStream = bfaPatternStream.select(
				new PatternSelectFunction<LogEvent, LogEvent>() {
					public LogEvent select(Map<String, List<LogEvent>> logEventsMap) throws Exception {
						List<LogEvent> leList = new ArrayList<LogEvent>();
						for (Map.Entry<String, List<LogEvent>> entry : logEventsMap.entrySet()) {
							LogEvent le = extractDataForBFAThreat(entry.getValue());
							leList.add(le);
						}
						if (leList.size() != 2) System.out.println("Something wrong while extracting BFA Threat !!");
						return extractBFAThreat(leList.get(0), leList.get(1));
					}
				}).setParallelism(TASK_PARALLELISM);	
		//outputBfaStream.print();
		
		DataStream<String> outputBfaJsonStream = outputBfaStream.map(new MapLogEventsToJson()).setParallelism(TASK_PARALLELISM);
		
		FlinkKafkaProducer010.FlinkKafkaProducer010Configuration bfaProducerConfig = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
				outputBfaJsonStream, TO_TOPIC, new KeyedSerializationSchemaWrapper(new SimpleStringSchema()), properties);
		// the following is necessary for at-least-once delivery guarantee
		bfaProducerConfig.setLogFailuresOnly(false); bfaProducerConfig.setFlushOnCheckpoint(true); bfaProducerConfig.setWriteTimestampToKafka(true);
		
		// execute program
		env.execute("CEP Threat Detection ALT - "+CONSUMER_GROUP);
	}
	
	private static LogEvent extractMFLThreat(List<LogEvent> value) {
		LogEvent le = new LogEvent();
		le.setRecordType("CORR");
		le.setEventCategory("MFL");
		
		String id = "";		
		LogEvent temp1 = value.get(0);
		LogEvent temp2 = value.get(value.size()-1);
		
		if (temp1 != null) {
			le.setTenantId(temp1.getTenantId());
			le.setUser(temp1.getUser());
			le.setMachine(temp1.getMachine());
			le.setStartTime(temp1.getTime());
			le.setTime(temp2.getTime());
			le.setTimeLong(LogEvent.convertToLong(temp2.getTime()));
		}		
		for(LogEvent temp3 : value) {
			if ("".equals(id)) {
				id = temp3.getId();
			} else {
				id = id + ":" + temp3.getId();
			}			
		}
		le.setId(id);		
		return le;
	}
	
	private static LogEvent extractDataForBFAThreat(List<LogEvent> value) {
		LogEvent le = new LogEvent();
		le.setRecordType("CORR");
		le.setEventCategory("BFA");
		
		String id = "";		
		LogEvent temp1 = value.get(0);
		LogEvent temp2 = value.get(value.size()-1);
		
		if (temp1 != null) {
			le.setTenantId(temp1.getTenantId());
			le.setUser(temp1.getUser());
			le.setMachine(temp1.getMachine());
			le.setStartTime(temp1.getStartTime());
			le.setTime(temp2.getTime());
			le.setTimeLong(LogEvent.convertToLong(temp2.getTime()));
		}		
		for(LogEvent temp3 : value) {
			if ("".equals(id)) {
				id = temp3.getId();
			} else {
				id = id + ":" + temp3.getId();
			}
		}
		le.setId(id);		
		return le;
	}
	
	private static LogEvent extractBFAThreat(LogEvent leMfl, LogEvent leSl) {
		LogEvent le = new LogEvent();
		le.setRecordType("CORR");
		le.setEventCategory("BFA");		
		le.setId(leMfl.getId()+"||"+leSl.getId());		
		le.setTenantId(leMfl.getTenantId());
		le.setUser(leMfl.getUser());
		le.setMachine(leMfl.getMachine());
		le.setStartTime(leMfl.getStartTime());
		le.setTime(leSl.getTime());
		le.setTimeLong(LogEvent.convertToLong(leSl.getTime()));		
		return le;
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
