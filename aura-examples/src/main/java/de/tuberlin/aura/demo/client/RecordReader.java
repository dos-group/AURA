package de.tuberlin.aura.demo.client;

import de.tuberlin.aura.core.iosystem.IOEvents;

public class RecordReader {

    public Record readRecord(IOEvents.DataBufferEvent data) {
        return new Record(data.data);
    }
}
