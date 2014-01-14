package de.tuberlin.aura.core.iosystem.buffer;

public class Util {

    public static int nextPowerOf2(int size) {
        return 1 << 32 - Integer.numberOfLeadingZeros(size - 1);
    }

    public static int getFixedSetCapacity(int elements, float loadFactor) {
        int capacity = (int) (elements / loadFactor);
        return Util.nextPowerOf2(capacity);
    }
}
