package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.redis.ProcessLevelConf;
import org.apache.flink.runtime.state.rescale.SubTaskMigrationInstruction;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class PostFetchRequestManager {
	private Map<Integer, PostFetchStateTable> tableMap = new ConcurrentHashMap<>();
	private boolean isWatching = false;
	private Map<Integer, Boolean> instructMap = new ConcurrentHashMap<>();
	private final Map<Integer, Integer> preOwnerMap = new ConcurrentHashMap<>();
	private final Map<Integer, Integer> newOwnerMap = new ConcurrentHashMap<>();

	private KafkaConsumer<String, String> requestMessageConsumer;
	private KafkaProducer<String, String> requestMessageProducer;
	private KafkaConsumer<String, byte[]> stateConsumer;
	private KafkaProducer<String, byte[]> stateProducer;
	private KafkaConsumer<String, byte[]> stateInfoConsumer;
	private KafkaProducer<String, byte[]> stateInfoProducer;
	private final Map<Integer, Boolean> keyGroupsToWatch = new ConcurrentHashMap<>();
	private final String kafkaRequestMessageTopic = ProcessLevelConf.getProperty(ProcessLevelConf.KAFKA_REQUEST_MESSAGE_TOPIC_KEY);
	private final String kafkaStateTopic = ProcessLevelConf.getProperty(ProcessLevelConf.KAFKA_STATE_TOPIC_KEY);
	private final String kafkaStateInfoTopic = ProcessLevelConf.getProperty(ProcessLevelConf.KAFKA_STATE_INFO_TOPIC_KEY);
	private String kafkaGroupId;
	private final String kafkaServers = ProcessLevelConf.getProperty(ProcessLevelConf.KAFKA_HOSTS_KEY);

	private static PostFetchRequestManager instance;

	private static class RequestMessage {
		int targetSubTaskIndex;
		int fromSubTaskIndex;
		int keyGroup;
		int keyBinHash;
		long timeStampCreate;

		RequestMessage(
			int targetSubTaskIndex,
			int fromSubTaskIndex,
			int keyGroup,
			int keyBinHash,
			long timeStampCreate) {

			this.targetSubTaskIndex = targetSubTaskIndex;
			this.fromSubTaskIndex = fromSubTaskIndex;
			this.keyGroup = keyGroup;
			this.keyBinHash = keyBinHash;
			this.timeStampCreate = timeStampCreate;
		}

		@Override
		public String toString() {
			return "" +
				targetSubTaskIndex + ":" +
				fromSubTaskIndex + ":" +
				keyGroup + ":" +
				keyBinHash + ":" +
				timeStampCreate;
		}

		static RequestMessage fromString(String str) {
			String[] strings = str.split(":");
			return new RequestMessage(
				Integer.parseInt(strings[0]),
				Integer.parseInt(strings[1]),
				Integer.parseInt(strings[2]),
				Integer.parseInt(strings[3]),
				Long.parseLong(strings[4])
			);
		}
	}

	private PostFetchRequestManager() {
		initProducer();
		initConsumer();
	}

	static synchronized PostFetchRequestManager getInstance() {
		if (instance == null) {
			instance = new PostFetchRequestManager();
		}
		return instance;
	}

	void registerTableAndTryStartWatch(PostFetchStateTable table) {
		tableMap.put(table.subtaskIndex, table);
		instructMap.put(table.subtaskIndex, false);
		System.out.println("Register table with subtask index: " + table.subtaskIndex);
		tryStartWatch();
	}

	private void initProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", kafkaServers);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		requestMessageProducer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
		stateProducer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
		stateInfoProducer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
	}

	private void initConsumer() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", kafkaServers);
		properties.put("session.timeout.ms", 60000 * 10); // 10 minutes
		try {
			kafkaGroupId = InetAddress.getLocalHost().getHostName() + RandomStringUtils.random(10, true, true);
		} catch (UnknownHostException e) {
			kafkaGroupId = "postFetchRequestTopic-group" + RandomStringUtils.random(10, true, true);
		}
		properties.put("group.id", kafkaGroupId);

		requestMessageConsumer = new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
		stateConsumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
		stateInfoConsumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
	}

	void tryUpdate(SubTaskMigrationInstruction instruction, int subTaskIndex) {
		if (!instructMap.get(subTaskIndex)) {
			update(instruction);
			instructMap.put(subTaskIndex, true);
		}
	}

	private void tryStartWatch() {
		if (!isWatching) {
			isWatching = true;
			CompletableFuture.runAsync(this::watchRequests);
//			CompletableFuture.runAsync(this::watchStates);
			CompletableFuture.runAsync(this::watchStatesInfo);
		}
	}

	private void watchRequests() {
		try {
			Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			System.out.println("Watch requests started. At " + System.currentTimeMillis());
			requestMessageConsumer.subscribe(Collections.singleton(kafkaRequestMessageTopic));
			requestMessageConsumer.seekToEnd(Collections.emptyList());
			System.out.println("Subscribe to topic: " + kafkaRequestMessageTopic);
			System.out.println("Kafka Group ID: " + kafkaGroupId);
			System.out.println("Thread Priority: " + Thread.currentThread().getPriority());

			while (isWatching) {
				ConsumerRecords<String, String> records = requestMessageConsumer.poll(30);
				for (ConsumerRecord<String, String> record : records) {
//					System.out.println("-poll from kafka: " + record.value());
					RequestMessage requestMessage = RequestMessage.fromString(record.value());
					if (tableMap.containsKey(requestMessage.targetSubTaskIndex)) {
						System.out.println("GetRequest: " + requestMessage +
							". at: " + System.currentTimeMillis() +
							". record timestamp: " + record.timestamp() + ", type: " + record.timestampType());

						tableMap.get(requestMessage.targetSubTaskIndex).invokeByRequestManager(requestMessage.fromSubTaskIndex, requestMessage.keyGroup, requestMessage.keyBinHash);
//						System.out.println("invokeByRequestManager " + requestMessage + " finished" + System.currentTimeMillis());
					}
				}
			}
			System.out.println("Watch requests finished. Topic: " + kafkaRequestMessageTopic);
		} catch (Exception e) {
			e.printStackTrace();
			stopWatch();
			System.out.println("Watch requests finished exceptionally. Topic: " + kafkaRequestMessageTopic);
		}
	}

	private void watchStates() {
		try {
			Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			System.out.println("Watch states started. At " + System.currentTimeMillis());
			stateConsumer.subscribe(Collections.singleton(kafkaStateTopic));
			stateConsumer.seekToEnd(Collections.emptyList());
			System.out.println("Subscribe to topic: " + kafkaStateTopic);
			System.out.println("Kafka Group ID: " + kafkaGroupId);
			System.out.println("Thread Priority: " + Thread.currentThread().getPriority());

			while (isWatching) {
				ConsumerRecords<String, byte[]> records = stateConsumer.poll(30);
				for (ConsumerRecord<String, byte[]> record : records) {
					CompletableFuture.runAsync(() -> {
						byte[] data = record.value();
						int targetSubTaskIndex = PostFetchByteConverter.getSubtaskIndexFromStateBytes(data);
//						System.out.println("-get state for: " + targetSubTaskIndex);
						PostFetchStateTable table = tableMap.get(targetSubTaskIndex);
						if (table != null) {
//							System.out.println("GetState for: " + targetSubTaskIndex +
//								". at: " + System.currentTimeMillis() +
//								". record timestamp: " + record.timestamp() + ", type: " + record.timestampType());
							int keyGroup = PostFetchByteConverter.getKeyGroupFromStateBytes(data);
							int keyBinHash = PostFetchByteConverter.getKeyBinHashFromStateBytes(data);
							byte[] stateData = PostFetchByteConverter.getStateDataFromStateBytes(data);
							table.onReceivingState(keyGroup, keyBinHash, stateData);
//							System.out.println("invokeOnReceivingState finished" + System.currentTimeMillis());
						}
					});
				}
			}
			System.out.println("Watch states finished. Topic: " + kafkaStateTopic);
		} catch (Exception e) {
			e.printStackTrace();
			stopWatch();
			System.out.println("Watch states finished exceptionally. Topic: " + kafkaStateTopic);
		}
	}

	private void watchStatesInfo() {
		try {
			Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			System.out.println("Watch states started. At " + System.currentTimeMillis());
			stateInfoConsumer.subscribe(Collections.singleton(kafkaStateInfoTopic));
			stateInfoConsumer.seekToEnd(Collections.emptyList());
			System.out.println("Subscribe to topic: " + kafkaStateInfoTopic);
			System.out.println("Kafka Group ID: " + kafkaGroupId);
			System.out.println("Thread Priority: " + Thread.currentThread().getPriority());

			while (isWatching) {
				ConsumerRecords<String, byte[]> records = stateInfoConsumer.poll(30);
				for (ConsumerRecord<String, byte[]> record : records) {
					CompletableFuture.runAsync(() -> {
						byte[] data = record.value();
						int targetSubTaskIndex = PostFetchByteConverter.getSubtaskIndexFromStateBytes(data);
//						System.out.println("-get state info for: " + targetSubTaskIndex);
						PostFetchStateTable table = tableMap.get(targetSubTaskIndex);
						if (table != null) {
//							System.out.println("GetStateInfo for: " + targetSubTaskIndex +
//								". at: " + System.currentTimeMillis() +
//								". record timestamp: " + record.timestamp() + ", type: " + record.timestampType());
							int keyGroup = PostFetchByteConverter.getKeyGroupFromStateBytes(data);
							int keyBinHash = PostFetchByteConverter.getKeyBinHashFromStateBytes(data);
							table.onReceivingStateInfo(keyGroup, keyBinHash);
//							System.out.println("onReceivingStateInfo finished" + System.currentTimeMillis());
						}
					});
				}
			}
			System.out.println("Watch states info finished. Topic: " + kafkaStateInfoTopic);
		} catch (Exception e) {
			e.printStackTrace();
			stopWatch();
			System.out.println("Watch states info finished exceptionally. Topic: " + kafkaStateInfoTopic);
		}
	}

	void publishRequest(int keyGroup, int keyBin, int fromSubTask) {
		try {
			int targetSubTask;
			try {
				targetSubTask = preOwnerMap.get(keyGroup);
				if (targetSubTask == fromSubTask) {
					targetSubTask = newOwnerMap.get(keyGroup);
				}
			} catch (Exception e) {
				e.printStackTrace();
				targetSubTask = 0;
			}
			RequestMessage requestMessage = new RequestMessage(targetSubTask, fromSubTask, keyGroup, keyBin, System.currentTimeMillis());
			requestMessageProducer.send(new ProducerRecord<>(
				kafkaRequestMessageTopic,
				requestMessage.toString()));
//			producer.flush();
			System.out.println("publishRequest: " + requestMessage + ". at: " + System.currentTimeMillis());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	void publishState(int targetSubtaskIndex, int keyGroup, int keyBin, byte[] stateData) {
		byte[] data = PostFetchByteConverter.keyBinStatesToBytes(targetSubtaskIndex, keyGroup, keyBin, stateData);
		stateProducer.send(new ProducerRecord<>(
			kafkaStateTopic,
			data
		));
		System.out.println("publishState: " + keyGroup + "-" + keyBin + ". at: " + System.currentTimeMillis());
	}

	void publishStateInfo(int targetSubtaskIndex, int keyGroup, int keyBin) {
		byte[] data = PostFetchByteConverter.keyBinStatesInfoToBytes(targetSubtaskIndex, keyGroup, keyBin);
		stateInfoProducer.send(new ProducerRecord<>(
			kafkaStateInfoTopic,
			data
		));
		System.out.println("publishStateInfo: " + keyGroup + "-" + keyBin + ". at: " + System.currentTimeMillis());
	}

	void stopWatch() {
		isWatching = false;
		requestMessageConsumer.unsubscribe();
		requestMessageConsumer.close();
		stateConsumer.unsubscribe();
		stateConsumer.close();
		stateInfoConsumer.unsubscribe();
		stateInfoConsumer.close();
	}

	void finishCurrentFetchStage(int subtaskIndex) {
		instructMap.put(subtaskIndex, false);
	}

	public void update(SubTaskMigrationInstruction instruction) {
		for (Map.Entry<Integer, Integer> entry : instruction.getPreOwnerMap().entrySet()) {
			preOwnerMap.put(entry.getKey(), entry.getValue());
		}
		for (Map.Entry<Integer, Integer> entry : instruction.getNewOwnerMap().entrySet()) {
			newOwnerMap.put(entry.getKey(), entry.getValue());
		}
	}
}
