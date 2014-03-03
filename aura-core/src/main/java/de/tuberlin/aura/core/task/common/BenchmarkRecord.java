package de.tuberlin.aura.core.task.common;

public class BenchmarkRecord {
    public long time;

    public BenchmarkRecord() {
        this.time = System.currentTimeMillis();
    }
}
