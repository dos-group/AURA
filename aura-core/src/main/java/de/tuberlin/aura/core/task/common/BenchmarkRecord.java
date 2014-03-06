package de.tuberlin.aura.core.task.common;

import java.util.UUID;

public class BenchmarkRecord {
    public long time;

    public BenchmarkRecord() {
        this.time = System.currentTimeMillis();
    }

    public static class SanityBenchmarkRecord extends BenchmarkRecord {
        public UUID nextTask;

        public SanityBenchmarkRecord() {

        }

        public SanityBenchmarkRecord(UUID nextTask) {
            this.nextTask = nextTask;
        }
    }
}


