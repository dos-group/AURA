package de.tuberlin.aura.core.dataflow.operators.descriptors;

import java.io.Serializable;
import java.util.UUID;

import de.tuberlin.aura.core.dataflow.udfs.contracts.IFunction;
import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;

/**
 *
 */
public final class DataflowNodeProperties implements Serializable {

    // ---------------------------------------------------
    // Dataflow Node Types.
    // ---------------------------------------------------

    public static enum InputArity {

        NULLARY,

        UNARY,

        BINARY,

        DYNAMIC
    }

    public static enum DataflowNodeType {

        MAP_TUPLE_OPERATOR(InputArity.UNARY),

        MAP_GROUP_OPERATOR(InputArity.UNARY),

        FLAT_MAP_TUPLE_OPERATOR(InputArity.UNARY),

        FLAT_MAP_GROUP_OPERATOR(InputArity.UNARY),

        FILTER_OPERATOR(InputArity.UNARY),

        UNION_OPERATOR(InputArity.BINARY),

        DIFFERENCE_OPERATOR(InputArity.BINARY),

        DISTINCT_OPERATOR(InputArity.UNARY),

        HASH_JOIN_OPERATOR(InputArity.BINARY),

        MERGE_JOIN_OPERATOR(InputArity.BINARY),

        GROUP_BY_OPERATOR(InputArity.UNARY),

        SORT_OPERATOR(InputArity.UNARY),

        FOLD_OPERATOR(InputArity.UNARY),

        REDUCE_OPERATOR(InputArity.UNARY),

        UDF_SOURCE(InputArity.NULLARY),

        FILE_SOURCE(InputArity.NULLARY),

        STREAM_SOURCE(InputArity.NULLARY),

        UDF_SINK(InputArity.UNARY),

        FILE_SINK(InputArity.UNARY),

        STREAM_SINK(InputArity.UNARY),

        // ---------------------------------------------------

        IMMUTABLE_DATASET(InputArity.DYNAMIC),

        MUTABLE_DATASET(InputArity.DYNAMIC);

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        public InputArity operatorInputArity;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        DataflowNodeType(final InputArity operatorInputArity) {
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

    public final DataflowNodeType operatorType;

    public final String instanceName;

    public final int localDOP;

    public final int globalDOP;

    public final int[][] partitioningKeys;

    public final Partitioner.PartitioningStrategy strategy;

    public TypeInformation input1Type;

    public TypeInformation input2Type;

    public TypeInformation outputType;

    public final Class<? extends IFunction> function;

    public final int[][] groupByKeyIndices;

    public final int[][] joinKeyIndices1;

    public final int[][] joinKeyIndices2;

    public final int[][] sortKeyIndices;

    public final SortOrder sortOrder;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DataflowNodeProperties(final UUID operatorUID, final String instanceName) {
        this(operatorUID, null, 0, null, null, 0, instanceName, null, null, null, null, null, null, null, null, null);
    }

    public DataflowNodeProperties(final UUID operatorUID,
                                  final DataflowNodeType operatorType,
                                  final int localDOP,
                                  final int[][] partitioningKeys,
                                  final Partitioner.PartitioningStrategy strategy,
                                  final int globalDOP,
                                  final String instanceName,
                                  final TypeInformation input1Type,
                                  final TypeInformation input2Type,
                                  final TypeInformation outputType,
                                  final Class<? extends IFunction> function,
                                  int[][] groupByKeyIndices,
                                  final int[][] keyIndices1,
                                  final int[][] keyIndices2,
                                  final int[][] sortKeyIndices,
                                  final SortOrder sortOrder) {
        // sanity check.
        if (operatorUID == null)
            throw new IllegalArgumentException("operatorUID == null");

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

        this.groupByKeyIndices = groupByKeyIndices;

        this.joinKeyIndices1 = keyIndices1;

        this.joinKeyIndices2 = keyIndices2;

        this.sortKeyIndices = sortKeyIndices;

        this.sortOrder = sortOrder;
    }
}
