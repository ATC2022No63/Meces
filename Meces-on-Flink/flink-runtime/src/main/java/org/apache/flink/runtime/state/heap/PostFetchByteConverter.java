package org.apache.flink.runtime.state.heap;

public class PostFetchByteConverter {
	public static byte[] keyGroupAndBinHashToBytes(int keyGroup, int binHash) {
		byte[] bytes = new byte[9];
		for (int i = 0; i < 4; i++) {
			bytes[i] = (byte) (keyGroup >>> (i * 8));
		}
		for (int i = 4; i < 8; i++) {
			bytes[i] = (byte) (binHash >>> ((i - 4) * 8));
		}
		bytes[8] = 0;

		return bytes;
	}

	private static int getIntFromBytes(byte[] data, int startIndex, int len) {
		int res = 0;
		for (int i = startIndex; i < startIndex + len; i++){
			int n = data[i] & 0xff;
			res += n << (i * 8);
		}
		return res;
	}

	static byte[] subtaskIndexToBytes(int targetSubTask, int fromSubTask) {
		byte[] bytes = new byte[9];
		for (int i = 0; i < 4; i++) {
			bytes[i] = (byte) (targetSubTask >>> (i * 8));
		}
		for (int i = 4; i < 8; i++) {
			bytes[i] = (byte) (fromSubTask >>> ((i - 4) * 8));
		}
		bytes[8] = 1;

		return bytes;
	}

	static byte[] keyGroupIndexToBytes(int keyGroupIndex) {
		byte[] bytes = new byte[4];
		for (int i = 0; i < 4; i++) {
			bytes[i] = (byte) (keyGroupIndex >>> (i * 8));
		}

		return bytes;
	}

	static byte[] keyBinStatesToBytes(
		int targetSubtaskIndex,
		int keyGroup,
		int binHash,
		byte[] stateData) {
		byte[] bytes = new byte[stateData.length + 12];
		for (int i = 0; i < 4; i++) {
			bytes[i] = (byte) (targetSubtaskIndex >>> (i * 8));
		}
		for (int i = 4; i < 8; i++) {
			bytes[i] = (byte) (keyGroup >>> ((i - 4) * 8));
		}
		for (int i = 8; i < 12; i++) {
			bytes[i] = (byte) (binHash >>> ((i - 8) * 8));
		}
		System.arraycopy(
			stateData, 0,
			bytes, 12,
			stateData.length
		);
		return bytes;
	}

	static byte[] keyBinStatesInfoToBytes(
		int targetSubtaskIndex,
		int keyGroup,
		int binHash) {
		byte[] bytes = new byte[12];
		for (int i = 0; i < 4; i++) {
			bytes[i] = (byte) (targetSubtaskIndex >>> (i * 8));
		}
		for (int i = 4; i < 8; i++) {
			bytes[i] = (byte) (keyGroup >>> ((i - 4) * 8));
		}
		for (int i = 8; i < 12; i++) {
			bytes[i] = (byte) (binHash >>> ((i - 8) * 8));
		}
		return bytes;
	}

	static int getSubtaskIndexFromStateBytes(byte[] data) {
		return getIntFromBytes(data, 0, 4);
	}

	static int getKeyGroupFromStateBytes(byte[] data) {
		return getIntFromBytes(data, 4, 4);
	}

	static int getKeyBinHashFromStateBytes(byte[] data) {
		return getIntFromBytes(data, 8, 4);
	}

	static byte[] getStateDataFromStateBytes(byte[] data) {
		// TODO: reuse data, not copy
		byte[] stateData = new byte[data.length - 12];
		System.arraycopy(
			data, 12,
			stateData, 0,
			stateData.length
		);
		return stateData;
	}
}
