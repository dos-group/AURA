package de.tuberlin.aura.core.processing.api;

import java.io.Serializable;
import java.util.UUID;

import de.tuberlin.aura.core.processing.udfs.contracts.IFunction;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;

/**
 *
 */
public final class OperatorProperties implements Serializable {

    public static enum OperatorInputArity {

        NULLARY,

        UNARY,

        BINARY
    }

    public static enum PhysicalOperatorType {

        MAP_TUPLE_OPERATOR(OperatorInputArity.UNARY),

        MAP_GROUP_OPERATOR(OperatorInputArity.UNARY),

        FLAT_MAP_TUPLE_OPERATOR(OperatorInputArity.UNARY),

        FLAT_MAP_GROUP_OPERATOR(OperatorInputArity.UNARY),

        FILTER_OPERATOR(OperatorInputArity.UNARY),

        UNION_OPERATOR(OperatorInputArity.BINARY),

        DIFFERENCE_OPERATOR(OperatorInputArity.BINARY),

        HASH_JOIN_OPERATOR(OperatorInputArity.BINARY),

        MERGE_JOIN_OPERATOR(OperatorInputArity.BINARY),

        GROUP_BY_OPERATOR(OperatorInputArity.UNARY),

        SORT_OPERATOR(OperatorInputArity.UNARY),

        REDUCE_OPERATOR(OperatorInputArity.UNARY),

        UDF_SOURCE(OperatorInputArity.NULLARY),

        FILE_SOURCE(OperatorInputArity.NULLARY),

        STREAM_SOURCE(OperatorInputArity.NULLARY),

        UDF_SINK(OperatorInputArity.UNARY),

        FILE_SINK(OperatorInputArity.UNARY),

        STREAM_SINK(OperatorInputArity.UNARY);

        public OperatorInputArity operatorInputArity;

        PhysicalOperatorType(final OperatorInputArity operatorInputArity) {
            this.operatorInputArity = operatorInputArity;
        }
    }

    public static enum SortOrder {

        ASCENDING,

        DESCENDING
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final long serialVersionUID = -1L;

    public final UUID operatorUID;

    public final PhysicalOperatorType operatorType;

    public final String instanceName;

    public final int localDOP;

    public final int globalDOP;

    public final int[][] partitioningKeys;

    public final Partitioner.PartitioningStrategy strategy;

    public TypeInformation input1Type;

    public TypeInformation input2Type;

    public TypeInformation outputType;

    public final Class<? extends IFunction> function;

    public final int[][] keyIndices1;

    public final int[][] keyIndices2;

    public final int[][] sortKeyIndices;

    public final SortOrder sortOrder;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public OperatorProperties(final UUID operatorUID,
                              final PhysicalOperatorType operatorType,
                              final int localDOP,
                              final int[][] partitioningKeys,
                              final Partitioner.PartitioningStrategy strategy,
                              final int globalDOP,
                              final String instanceName,
                              final TypeInformation input1Type,
                              final TypeInformation input2Type,
                              final TypeInformation outputType,
                              final Class<? extends IFunction> function,
                              final int[][] keyIndices1,
                              final int[][] keyIndices2,
                              final int[][] sortKeyIndices,
                              final SortOrder sortOrder) {
        // sanity check.
        if (operatorUID == null)
            throw new IllegalArgumentException("operatorUID == null");
        if (operatorType == null)
            throw new IllegalArgumentException("operatorType == null");
        if (instanceName == null)
            throw new IllegalArgumentException("instanceName == null");

        this.operatorUID = operatorUID;

        this.operatorType = operatorType;

        this.localDOP = localDOP;

        this.partitioningKeys = partitioningKeys;

        this.strategy = strategy;

        this.globalDOP = globalDOP;

        this.instanceName = instanceName;

        this.input1Type = input1Type;

        this.input2Type = input2Type;

        this.outputType = outputType;

        this.function = function;

        this.keyIndices1 = keyIndices1;

        this.keyIndices2 = keyIndices2;

        this.sortKeyIndices = sortKeyIndices;

        this.sortOrder = sortOrder;
    }

    /*public OperatorProperties(final UUID operatorUID,
                              final PhysicalOperatorType operatorType,
                              final int localDOP,
                              final int[] keys,
                              final Partitioner.PartitioningStrategy strategy,
                              final int globalDOP,
                              final String instanceName,
                              final Class<?> input1Type,
                              final Class<?> input2Type,
                              final Class<?> outputType,
                              final Class<? extends IFunction> function,
                              final int[] keyIndices1,
                              final int[] keyIndices2,
                              final int[] sortKeyIndices,
                              final SortOrder sortOrder) {
        // sanity check.
        if (operatorUID == null)
            throw new IllegalArgumentException("operatorUID == null");
        if (operatorType == null)
            throw new IllegalArgumentException("operatorType == null");
        if (instanceName == null)
            throw new IllegalArgumentException("instanceName == null");

        this.operatorUID = operatorUID;

        this.operatorType = operatorType;

        this.localDOP = localDOP;

        this.keys = keys;

        this.strategy = strategy;

        this.globalDOP = globalDOP;

        this.instanceName = instanceName;

        this.input1Type = input1Type;

        this.input2Type = input2Type;

        this.outputType = outputType;

        this.function = function;

        this.keyIndices1 = keyIndices1;

        this.keyIndices2 = keyIndices2;

        this.sortKeyIndices = sortKeyIndices;

        this.sortOrder = sortOrder;
    }*/
}
