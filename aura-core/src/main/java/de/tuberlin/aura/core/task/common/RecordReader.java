package de.tuberlin.aura.core.task.common;

import de.tuberlin.aura.core.iosystem.IOEvents;

public class RecordReader {

    public Record readRecord(IOEvents.DataBufferEvent data) {
        return new Record(data.data);
    }
}
