package org.apache.flink.runtime.state;

import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

public class KeyGroupRangeDifferenceComputation {
	public static LinkedList<Integer> keyGroupRangesToLinkedList(List<KeyGroupRange> keyGroupRanges) {
		LinkedList<Integer> result = new LinkedList<>();
		keyGroupRanges
			.stream()
			.flatMapToInt(keyGroupRange -> IntStream.rangeClosed(
				keyGroupRange.getStartKeyGroup(),
				keyGroupRange.getEndKeyGroup()))
			.forEach(result::addLast);
		return result;
	}

	public static List<KeyGroupRange> linkedListToKeyGroupRanges(LinkedList<Integer> list) {
		List<KeyGroupRange> result = new ArrayList<>();
		if (!list.isEmpty()) {
			Iterator<Integer> ite = list.iterator();
			int curStart = ite.next();
			int curEnd = curStart;
			while (ite.hasNext()) {
				int value = ite.next();
				if (value == curEnd + 1) {
					curEnd = value;
				} else {
					KeyGroupRange keyGroupRange = new KeyGroupRange(curStart, curEnd);
					result.add(keyGroupRange);
					curStart = value;
					curEnd = value;
				}
			}

			KeyGroupRange keyGroupRange = new KeyGroupRange(curStart, curEnd);
			result.add(keyGroupRange);
		}
		return result;
	}

	public static void getDisposeAndFetching(
		List<KeyGroupRange> oldKGs,
		List<KeyGroupRange> newKGs,
		List<KeyGroupRange> disposed,
		List<KeyGroupRange> fetching,
		List<KeyGroupRange> remained) {
		Preconditions.checkArgument(oldKGs.size() != 0 || newKGs.size() != 0);
		if (oldKGs.size() == 0) {
			disposed.clear();
			fetching.addAll(newKGs);
			remained.clear();
			return;
		} else if (newKGs.size() == 0) {
			disposed.addAll(oldKGs);
			fetching.clear();
			remained.clear();
			return;
		}

		int maxKeyGroup = Math.max(oldKGs.get(oldKGs.size() - 1).getEndKeyGroup(), newKGs.get(newKGs.size() - 1).getEndKeyGroup());
		boolean[] oldMarks = new boolean[maxKeyGroup + 1];
		boolean[] newMarks = new boolean[maxKeyGroup + 1];
		// TODO: check here
		boolean[] doubleCheck = new boolean[maxKeyGroup + 1];
		oldKGs.forEach(oldKG -> {
			oldMarks[oldKG.getStartKeyGroup()] = true;
			oldMarks[oldKG.getEndKeyGroup()] = true;
			if (oldKG.getNumberOfKeyGroups() == 1) {
				doubleCheck[oldKG.getEndKeyGroup()] = true;
			}
		});
		newKGs.forEach(newKG -> {
			newMarks[newKG.getStartKeyGroup()] = true;
			newMarks[newKG.getEndKeyGroup()] = true;
			if (newKG.getNumberOfKeyGroups() == 1) {
				doubleCheck[newKG.getEndKeyGroup()] = true;
			}
		});
		boolean isInOld = false, isInNew = false;
		int pointer = 0, previousPointer = 0, mode = 0; // mode : 0 - neither, 1 - old, 2 - new, 3 - both
		while (pointer <= maxKeyGroup) {
			if (oldMarks[pointer]) {
				isInOld = !isInOld;
			}
			if (newMarks[pointer]) {
				isInNew = !isInNew;
			}
			if (oldMarks[pointer] || newMarks[pointer]) {
				switch (mode) {
					case 1:
						disposed.add(new KeyGroupRange(previousPointer, pointer - (isInNew ? 1 : 0)));
						previousPointer = pointer + (isInNew ? 0 : 1);
						break;
					case 2:
						fetching.add(new KeyGroupRange(previousPointer, pointer - (isInOld ? 1 : 0)));
						previousPointer = pointer + (isInOld ? 0 : 1);
						break;
					case 3:
						remained.add(new KeyGroupRange(previousPointer, pointer));
						previousPointer = pointer + 1;
						break;
					case 0:
						previousPointer = pointer;
						break;
					default:
						break;
				}
				if (isInOld && isInNew) {
					mode = 3;
				} else if (isInOld) {
					mode = 1;
				} else if (isInNew) {
					mode = 2;
				} else {
					mode = 0;
				}
			}
			if (doubleCheck[pointer]) {
				doubleCheck[pointer] = false;
			} else {
				pointer++;
			}
		}
	}
}
