package de.tuberlin.aura.core.dataflow.operators.descriptors;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

import de.tuberlin.aura.core.record.Partitioner;
import de.tuberlin.aura.core.record.TypeInformation;


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

        MUTABLE_DATASET(InputArity.DYNAMIC),


        // ---------------------------------------------------

        LOOP_CONTROL_OPERATOR(InputArity.UNARY);

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

    public final DataflowNodeType type;

    public final String instanceName;

    public final int localDOP;

    public final int globalDOP;

    public final int[][] partitioningKeys;

    public final Partitioner.PartitioningStrategy strategy;

    public TypeInformation input1Type;

    public TypeInformation input2Type;

    public TypeInformation outputType;

    public final String functionTypeName;

    public final int[][] groupByKeyIndices;

    public final int[][] keyIndices1;

    public final int[][] keyIndices2;

    public final int[][] sortKeyIndices;

    public final SortOrder sortOrder;

    public final List<UUID> broadcastVars;

    public final int[][] datasetKeyIndices;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public DataflowNodeProperties(final UUID operatorUID, final String instanceName) {
        this(operatorUID, null, 0, null, null, 0, instanceName, null, null, null, null, null, null, null, null, null, null, null);
    }

    public DataflowNodeProperties(final UUID operatorUID,
                                  final DataflowNodeType type,
                                  final int localDOP,
                                  final int[][] partitioningKeys,
                                  final Partitioner.PartitioningStrategy strategy,
                                  final int globalDOP,
                                  final String instanceName,
                                  final TypeInformation input1Type,
                                  final TypeInformation input2Type,
                                  final TypeInformation outputType,
                                  final String functionTypeName,
                                  final int[][] groupByKeyIndices,
                                  final int[][] keyIndices1,
                                  final int[][] keyIndices2,
                                  final int[][] sortKeyIndices,
                                  final SortOrder sortOrder,
                                  final List<UUID> broadcastVars,
                                  final int[][] datasetKeyIndices) {
        // sanity check.
        if (operatorUID == null)
            throw new IllegalArgumentException("operatorUID == null");

        this.operatorUID = operatorUID;

        this.type = type;

        this.localDOP = localDOP;

        this.partitioningKeys = partitioningKeys;

        this.strategy = strategy;

        this.globalDOP = globalDOP;

        this.instanceName = instanceName;

        this.input1Type = input1Type;

        this.input2Type = input2Type;

        this.outputType = outputType;

        this.functionTypeName = functionTypeName;

        this.groupByKeyIndices = groupByKeyIndices;

        this.keyIndices1 = keyIndices1;

        this.keyIndices2 = keyIndices2;

        this.sortKeyIndices = sortKeyIndices;

        this.sortOrder = sortOrder;

        this.broadcastVars = broadcastVars;

        this.datasetKeyIndices = datasetKeyIndices;
    }
}