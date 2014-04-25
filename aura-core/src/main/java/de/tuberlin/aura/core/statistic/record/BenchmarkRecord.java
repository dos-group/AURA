package de.tuberlin.aura.core.statistic.record;

import java.util.UUID;

public class BenchmarkRecord {

    public long time;

    public BenchmarkRecord() {
        this.time = System.currentTimeMillis();
    }

    public static class SanityBenchmarkRecord extends BenchmarkRecord {

        public UUID nextTask;

        public SanityBenchmarkRecord() {}

        public SanityBenchmarkRecord(UUID nextTask) {
            this.nextTask = nextTask;
        }
    }

    public static class WordCountBenchmarkRecord extends BenchmarkRecord {

        public String word;

        public int count;

        public WordCountBenchmarkRecord() {}

        public WordCountBenchmarkRecord(String word, int count) {
            this.word = word;
            this.count = count;
        }
    }
}
