package de.tuberlin.aura.core.measurement.record;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.iosystem.IOEvents;

public class RecordReader {

    public static final Logger LOG = LoggerFactory.getLogger(RecordReader.class);

    public Record readRecord(IOEvents.TransferBufferEvent data) {
        // LOG.debug(data.messageID.toString());
        return new Record(data.buffer);
    }
}
