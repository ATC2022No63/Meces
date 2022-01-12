package org.apache.flink.runtime.state.rescale;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class SubTaskMigrationInstruction implements Serializable {
	private LinkedList<Integer> keyGroupsToDispose = new LinkedList<>();
	private LinkedList<Integer> keyGroupsToFetch = new LinkedList<>();
	private Map<Integer, Integer> preOwnerMap = new HashMap<>();
	private Map<Integer, Integer> newOwnerMap = new HashMap<>();

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Dispose:");
		keyGroupsToDispose.forEach(integer -> sb.append(integer).append(", "));
		sb.append("Fetch:");
		keyGroupsToFetch.forEach(integer -> sb.append(integer).append(", "));
		return sb.toString();
	}

	void addKeyGroupsToFetch(int keyGroup, int preOwnerIndex, int newOwnerIndex) {
		keyGroupsToFetch.addLast(keyGroup);
		preOwnerMap.put(keyGroup, preOwnerIndex);
		newOwnerMap.put(keyGroup, newOwnerIndex);
	}

	void addKeyGroupsToDispose(int keyGroup, int preOwnerIndex, int newOwnerIndex) {
		keyGroupsToDispose.addLast(keyGroup);
		preOwnerMap.put(keyGroup, preOwnerIndex);
		newOwnerMap.put(keyGroup, newOwnerIndex);
	}

	public LinkedList<Integer> getKeyGroupsToFetch() {
		return keyGroupsToFetch;
	}

	public LinkedList<Integer> getKeyGroupsToDispose() {
		return keyGroupsToDispose;
	}

	public Map<Integer, Integer> getPreOwnerMap() {
		return preOwnerMap;
	}

	public Map<Integer, Integer> getNewOwnerMap() {
		return newOwnerMap;
	}
}
