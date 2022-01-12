package org.apache.flink.runtime.microbatch;

import java.util.Random;

public class ArrayShuffle {
    private Random rand;

    public ArrayShuffle(int seed) {
        this.rand = new Random(seed);
    }

    public <T> void swap(T[] a, int i, int j) {
        T temp = a[i];
        a[i] = a[j];
        a[j] = temp;
    }

    public <T> void shuffle(T[] arr) {
        int length = arr.length;
        for (int i = length; i > 0; i--) {
            int randInd = rand.nextInt(i);
            swap(arr, randInd, i - 1);
        }
    }
}
