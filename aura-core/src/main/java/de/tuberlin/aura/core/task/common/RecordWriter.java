package de.tuberlin.aura.core.task.common;

import de.tuberlin.aura.core.iosystem.IOEvents;

public class RecordWriter {
    public void writeRecord(Record record, IOEvents.DataBufferEvent data) {
        record.serialize(data.data);
    }
}
