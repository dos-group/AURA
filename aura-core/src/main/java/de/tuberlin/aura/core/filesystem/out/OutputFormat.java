package de.tuberlin.aura.core.filesystem.out;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.Serializable;


public interface OutputFormat<IT> extends Serializable {

    void configure(Configuration parameters);

    void open(int taskNumber, int numTasks) throws IOException;

    void writeRecord(IT record) throws IOException;

    void close() throws IOException;
}
