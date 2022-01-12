package org.apache.flink.runtime.state.heap.pooled;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PooledHashMap<K, V> extends HashedMap<K, V> {
	private static int useCount = 0;
	private GenericObjectPool<PooledHashEntry<K, V>> entryPool;
	private int initialCapacity;
	public PooledHashMap(
		int initPoolSize,
		Class namespaceSerializerClass,
		Class keySerializerClass,
		Class stateSerializerClass) {

		super();
		if (entryPool == null) {
			entryPool = PoolFactory.getEntryPool(initPoolSize, namespaceSerializerClass, keySerializerClass, stateSerializerClass);
		}
		this.initialCapacity = initPoolSize;
	}

	@Override
	protected HashEntry<K, V> createEntry(HashEntry<K, V> next, int hashCode, K key, V value) {
		try {
			useCount++;
			if (useCount % 10000 == 0) {
				System.out.println("useCount: " + useCount);
				System.out.println("entry pool idle: " + entryPool.getNumIdle());
			}
			PooledHashEntry<K, V> entry = entryPool.borrowObject();
			entry.setFields(next, hashCode, key, value);
			return entry;
		} catch (Exception e) {
			e.printStackTrace();
			return new PooledHashEntry<>(next, hashCode, key, value);
		}
	}

	static class PooledHashEntry<K, V> extends HashEntry<K, V> {
		PooledHashEntry(
			HashEntry<K, V> next,
			int hashCode,
			K key,
			V value) {
			super(next, hashCode, key, value);
		}

		void setFields(
			HashEntry<K, V> next,
			int hashCode,
			K key,
			V value) {
			this.next = next;
			this.hashCode = hashCode;
			this.key = key;
			this.value = value;
		}
	}

	static class EntryPoolFactory<K, V> extends BasePooledObjectFactory<PooledHashEntry<K, V>> {

		@Override
		public PooledHashEntry<K, V> create() throws Exception {
			return new PooledHashEntry<>(null, 0, null, null);
		}

		@Override
		public PooledObject<PooledHashEntry<K, V>> wrap(PooledHashEntry<K, V> kvPooledHashEntry) {
			return new DefaultPooledObject<>(kvPooledHashEntry);
		}
	}

	static class PoolFactory {
		private static Map<Class, Map<Class, GenericObjectPool>> entryPoolMap = new HashMap<>();

		@SuppressWarnings("unchecked")
		static synchronized <K, V> GenericObjectPool<PooledHashEntry<K, V>> getEntryPool(
			int initialCapacity,
			Class namespaceSerializerClass,
			Class keySerializerClass,
			Class stateSerializerClass) {
//			TypeReference<K> keyTypeRef = new TypeReference<K>() {};
//			TypeReference<V> valueTypeRef = new TypeReference<V>() {};
			Map<Class, GenericObjectPool> subMap = entryPoolMap.computeIfAbsent(keySerializerClass, k -> new HashMap<>());

			return (GenericObjectPool<PooledHashEntry<K, V>>) subMap.computeIfAbsent(stateSerializerClass, v -> newEntryPool(initialCapacity));
		}

		static <K, V> GenericObjectPool<PooledHashEntry<K, V>> newEntryPool(
			int initialCapacity) {
			GenericObjectPoolConfig<PooledHashEntry<K, V>> config = new GenericObjectPoolConfig<>();
			config.setMinIdle(1000);
			config.setMaxIdle(initialCapacity);
			config.setMaxTotal(initialCapacity);
			GenericObjectPool<PooledHashEntry<K, V>> entryPool = new GenericObjectPool<>(new EntryPoolFactory<>(), config);
			CompletableFuture.runAsync(() -> {
				try {
					System.out.println("Start filling entry pool");
					for (int i = 0; i < initialCapacity; i++) {
						entryPool.addObject();
						if (i % 1000000 == 0) {
							System.out.println("PooledHashMap pool size grow to " + i);
//							Thread.sleep(2000);
//							System.gc();
						}
					}
//					System.gc();
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			return entryPool;
		}
	}

	public abstract static class TypeReference<T> {
		// https://stackoverflow.com/questions/3397160/how-can-i-pass-a-class-as-parameter-and-return-a-generic-collection-in-java
		private final Type type;

		TypeReference() {
			Type superclass = getClass().getGenericSuperclass();
			if (superclass instanceof Class<?>) {
				throw new RuntimeException("Missing type parameter.");
			}
			this.type = ((ParameterizedType) superclass).getActualTypeArguments()[0];
		}

		public Type getType() {
			return this.type;
		}
	}
}
