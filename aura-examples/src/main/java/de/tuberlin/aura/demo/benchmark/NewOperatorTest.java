package de.tuberlin.aura.demo.benchmark;

import java.util.UUID;

import de.tuberlin.aura.core.iosystem.IOEvents;
import de.tuberlin.aura.core.task.common.BenchmarkRecord;
import de.tuberlin.aura.core.task.common.Record;
import de.tuberlin.aura.core.task.common.RecordWriter;

/**
 * Created by teots on 3/5/14.
 */
public class NewOperatorTest {

    /**
     * @param args
     */
    public static void main(String[] args) {

        RecordWriter writer = new RecordWriter();

        long start = System.nanoTime();
        for (int i = 0; i < 10000; ++i) {
            byte[] data = new byte[64 * 1024];
            final IOEvents.DataBufferEvent outputBuffer = new IOEvents.DataBufferEvent(UUID.randomUUID(), UUID.randomUUID(), data);
            final Record<BenchmarkRecord> record = new Record<>(new BenchmarkRecord());
            writer.writeRecord(record, outputBuffer);
        }

        long end = Math.abs(System.nanoTime() - start);

        System.out.println(Long.toString(end / 1000000));
    }
}
