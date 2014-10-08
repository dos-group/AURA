package de.tuberlin.aura.taskmanager.hdfs;

import de.tuberlin.aura.core.descriptors.Descriptors;
import de.tuberlin.aura.core.filesystem.InputSplit;
import de.tuberlin.aura.core.protocols.ITM2WMProtocol;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskInputSplitProvider implements Serializable {

    private final Descriptors.AbstractNodeDescriptor nodeDescriptor;

    private final ITM2WMProtocol globalInputSplitProvider;

    private final AtomicInteger sequenceNumber = new AtomicInteger(0);

    public TaskInputSplitProvider(final Descriptors.AbstractNodeDescriptor nodeDescriptor,
                                  final ITM2WMProtocol globalInputSplitProvider) {

        this.nodeDescriptor = nodeDescriptor;

        this.globalInputSplitProvider = globalInputSplitProvider;
    }

    public InputSplit getNextInputSplit() {
        synchronized(globalInputSplitProvider) {
            return globalInputSplitProvider.requestNextInputSplit(nodeDescriptor.topologyID, nodeDescriptor.taskID, this.sequenceNumber.getAndIncrement());
        }
    }
}