package de.tuberlin.aura.core.task.common;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class MedianHelper {
    private static final Random RANDOM = new Random();

    private MedianHelper() {
        // This will never be called.
    }

    public static long findMedian(List<Long> list) {
        return findMedian(list, list.size() / 2);
    }

    private static long findMedian(List<Long> list, int pos) {
        LinkedList<Long> smaller = new LinkedList<>();
        LinkedList<Long> bigger = new LinkedList<>();

        long pivot = list.get(RANDOM.nextInt(list.size()));
        int equals = -1;

        for (long curNumber : list) {
            if (curNumber < pivot) {
                smaller.add(curNumber);
            } else if (curNumber > pivot) {
                bigger.add(curNumber);
            } else {
                equals++;
            }
        }

        if (pos >= smaller.size() && pos <= smaller.size() + equals) {
            return pivot;
        } else if (smaller.size() < pos) {
            return findMedian(bigger, pos - equals - smaller.size() - 1);
        } else {
            return findMedian(smaller, pos);
        }
    }

    public static double findMedianDouble(List<Double> list) {
        return findMedianDouble(list, list.size() / 2);
    }

    private static double findMedianDouble(List<Double> list, int pos) {
        LinkedList<Double> smaller = new LinkedList<>();
        LinkedList<Double> bigger = new LinkedList<>();

        double pivot = list.get(RANDOM.nextInt(list.size()));
        int equals = -1;

        for (double curNumber : list) {
            if (curNumber < pivot) {
                smaller.add(curNumber);
            } else if (curNumber > pivot) {
                bigger.add(curNumber);
            } else {
                equals++;
            }
        }

        if (pos >= smaller.size() && pos <= smaller.size() + equals) {
            return pivot;
        } else if (smaller.size() < pos) {
            return findMedianDouble(bigger, pos - equals - smaller.size() - 1);
        } else {
            return findMedianDouble(smaller, pos);
        }
    }
}
