package de.tuberlin.aura.core.task.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.tuberlin.aura.core.iosystem.IOEvents;

public class RecordWriter {

    public static final Logger LOG = LoggerFactory.getLogger(RecordWriter.class);

    public void writeRecord(Record record, IOEvents.TransferBufferEvent data) {
        // LOG.debug(data.messageID.toString());
        record.serialize(data.buffer);
    }
}
