package de.tuberlin.aura.demo.client;

import de.tuberlin.aura.core.iosystem.IOEvents;

public class RecordWriter {

    public void writeRecord(Record record, IOEvents.DataBufferEvent data) {
        record.serialize(data.data);
    }
}
