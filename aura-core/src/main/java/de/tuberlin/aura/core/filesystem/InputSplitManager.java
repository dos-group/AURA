package de.tuberlin.aura.core.filesystem;

import de.tuberlin.aura.core.filesystem.in.CSVInputFormat;
import de.tuberlin.aura.core.filesystem.in.InputFormat;
import de.tuberlin.aura.core.topology.Topology;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class InputSplitManager implements Serializable {

    private Map<UUID, InputSplitAssigner> inputSplitMap;

    public InputSplitManager() {
        this.inputSplitMap = new ConcurrentHashMap<>();
    }

    public void registerHDFSSource(final Topology.LogicalNode node) {
        if (node == null)
            throw new IllegalArgumentException("node == null");
        if (node.properties == null)
            throw new IllegalStateException("properties == null");

        //final InputFormat inputFormat = (InputFormat) node.properties.config.get("INPUT_FORMAT");

        final Path path = new Path((String)node.properties.config.get("HDFS_PATH"));
        final Class<?>[] fieldTypes = (Class<?>[]) node.properties.config.get("FIELD_TYPES");
        final InputFormat inputFormat = new CSVInputFormat(path, fieldTypes);

        final Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000/");
        inputFormat.configure(conf);

        if (inputFormat == null)
            throw new IllegalStateException("inputFormat == null");

        final InputSplit[] inputSplits;
        try {
            inputSplits = inputFormat.createInputSplits(node.properties.globalDOP);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        final InputSplitAssigner inputSplitAssigner = new LocatableInputSplitAssigner((FileInputSplit[])inputSplits);
        inputSplitMap.put(node.uid, inputSplitAssigner);
    }

    public synchronized InputSplit getInputSplitFromHDFSSource(final Topology.ExecutionNode exNode) {
        if (exNode == null)
            throw new IllegalArgumentException("exNode == null");

        final InputSplitAssigner inputSplitAssigner = inputSplitMap.get(exNode.logicalNode.uid);
        return inputSplitAssigner.getNextInputSplit(exNode);
    }
}