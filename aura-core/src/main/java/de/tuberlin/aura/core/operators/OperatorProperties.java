package de.tuberlin.aura.core.operators;

import de.tuberlin.aura.core.record.Partitioner;

import java.io.Serializable;
import java.util.UUID;

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

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final long serialVersionUID = -1L;

    public final UUID operatorUID;

    public final PhysicalOperatorType operatorType;

    public final String instanceName;

    public final int localDOP;

    public final int globalDOP;

    public final int[] keys;

    public final Partitioner.PartitioningStrategy strategy;

    public final Class<?> input1Type;

    public final Class<?> input2Type;

    public final Class<?> outputType;

    public final Class<?> udfFunction;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public OperatorProperties(final UUID operatorUID,
                              final PhysicalOperatorType operatorType,
                              final int localDOP,
                              final int[] keys,
                              final Partitioner.PartitioningStrategy strategy,
                              final int globalDOP,
                              final String instanceName,
                              final Class<?> input1Type,
                              final Class<?> input2Type,
                              final Class<?> outputType,
                              final Class<?> udfFunction) {

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

        this.udfFunction = udfFunction;
    }

    public OperatorProperties(final PhysicalOperatorType operatorType,
                              final int[] keys,
                              final Partitioner.PartitioningStrategy strategy,
                              final int globalDOP,
                              final String instanceName,
                              final Class<?> input1Type,
                              final Class<?> input2Type,
                              final Class<?> outputType) {

        this(UUID.randomUUID(), operatorType, 1, keys, strategy, globalDOP, instanceName, input1Type, input2Type, outputType, null);
    }

    public OperatorProperties(final PhysicalOperatorType operatorType,
                              final int[] keys,
                              final Partitioner.PartitioningStrategy strategy,
                              final int globalDOP,
                              final String instanceName,
                              final Class<?> udfFunction,
                              final Class<?> input1Type,
                              final Class<?> input2Type,
                              final Class<?> outputType) {

        this(UUID.randomUUID(), operatorType, 1, keys, strategy, globalDOP, instanceName, input1Type, input2Type, outputType, udfFunction);
    }

    public OperatorProperties(final PhysicalOperatorType operatorType,
                              final int globalDOP,
                              final String instanceName,
                              final Class<?> udfFunction,
                              final Class<?> input1Type,
                              final Class<?> input2Type,
                              final Class<?> outputType) {

        this(UUID.randomUUID(), operatorType, 1, null, null, globalDOP, instanceName, input1Type, input2Type, outputType, udfFunction);
    }
}
