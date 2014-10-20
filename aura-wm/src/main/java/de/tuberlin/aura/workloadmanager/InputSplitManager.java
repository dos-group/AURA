package de.tuberlin.aura.workloadmanager;

import de.tuberlin.aura.core.dataflow.operators.impl.HDFSSourcePhysicalOperator;
import de.tuberlin.aura.core.filesystem.FileInputSplit;
import de.tuberlin.aura.core.filesystem.InputSplit;
import de.tuberlin.aura.core.filesystem.InputSplitAssigner;
import de.tuberlin.aura.core.filesystem.LocatableInputSplitAssigner;
import de.tuberlin.aura.core.filesystem.in.CSVInputFormat;
import de.tuberlin.aura.core.filesystem.in.InputFormat;
import de.tuberlin.aura.core.topology.Topology;
import de.tuberlin.aura.workloadmanager.spi.IWorkloadManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class InputSplitManager implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final IWorkloadManager workloadManager;

    private Map<UUID, List<FileInputSplit>> inputSplitMap;

    private Map<UUID, InputSplitAssigner> inputSplitAssignerMap;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public InputSplitManager(final IWorkloadManager workloadManager) {
        // sanity check.
        if (workloadManager == null)
            throw new IllegalArgumentException("workloadManager == null");

        this.workloadManager = workloadManager;

        this.inputSplitMap = new ConcurrentHashMap<>();

        this.inputSplitAssignerMap = new ConcurrentHashMap<>();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public void registerHDFSSource(final Topology.LogicalNode node) {
        if (node == null)
            throw new IllegalArgumentException("node == null");
        if (node.propertiesList.get(0) == null)
            throw new IllegalStateException("properties == null");

        final Path path = new Path((String)node.propertiesList.get(0).config.get(HDFSSourcePhysicalOperator.HDFS_SOURCE_FILE_PATH));
        final Class<?>[] fieldTypes = (Class<?>[]) node.propertiesList.get(0).config.get(HDFSSourcePhysicalOperator.HDFS_SOURCE_INPUT_FIELD_TYPES);

        @SuppressWarnings("unchecked")
        final InputFormat inputFormat = new CSVInputFormat(path, fieldTypes);

        final Configuration conf = new Configuration();
        conf.set("fs.defaultFS", workloadManager.getConfig().getString("wm.io.hdfs.hdfs_url"));
        inputFormat.configure(conf);

        final List<FileInputSplit> inputSplits;
        try {
            inputSplits = Arrays.asList((FileInputSplit[]) inputFormat.createInputSplits(node.propertiesList.get(0).globalDOP));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        inputSplitMap.put(node.uid, inputSplits);

        final InputSplitAssigner inputSplitAssigner = new LocatableInputSplitAssigner(inputSplits);
        inputSplitAssignerMap.put(node.uid, inputSplitAssigner);
    }

    public synchronized List<FileInputSplit> getAllInputSplitsForLogicalHDFSSource(final Topology.LogicalNode node) {
        return inputSplitMap.get(node.uid);
    }

    public synchronized InputSplit getNextInputSplitForExecutionUnit(final Topology.ExecutionNode exNode) {
        if (exNode == null)
            throw new IllegalArgumentException("exNode == null");
        final InputSplitAssigner inputSplitAssigner = inputSplitAssignerMap.get(exNode.logicalNode.uid);
        return inputSplitAssigner.getNextInputSplit(exNode);
    }

}