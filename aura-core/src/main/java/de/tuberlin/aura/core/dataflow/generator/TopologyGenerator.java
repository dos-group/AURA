package de.tuberlin.aura.core.dataflow.generator;

import java.util.ArrayList;
import java.util.List;

import de.tuberlin.aura.core.common.utils.ArrayUtils;
import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.dataflow.operators.descriptors.DataflowAPI;
import de.tuberlin.aura.core.dataflow.operators.descriptors.DataflowNodeProperties;
import de.tuberlin.aura.core.topology.Topology;

/**
 *
 */
public final class TopologyGenerator implements IVisitor<DataflowAPI.DataflowNodeDescriptor> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final Topology.AuraTopologyBuilder topologyBuilder;

    private Topology.AuraTopologyBuilder.NodeConnector currentConnector;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public TopologyGenerator(final Topology.AuraTopologyBuilder topologyBuilder) {
        // sanity check.
        if (topologyBuilder == null)
            throw new IllegalArgumentException("topologyBuilder == null");

        this.topologyBuilder = topologyBuilder;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public TopologyGenerator generate(final DataflowAPI.DataflowNodeDescriptor element) {
        element.accept(this);
        return this;
    }

    @Override
    public void visit(final DataflowAPI.DataflowNodeDescriptor element) {

            if (element.input1 != null) {

                visit(element.input1);

                currentConnector.connectTo(
                        element.properties.instanceName,
                        selectEdgeTransferType(element.input1.properties, element.properties)
                );
            }

            if (element.input2 != null) {

                visit(element.input2);

                currentConnector.connectTo(
                        element.properties.instanceName,
                        selectEdgeTransferType(element.input2.properties, element.properties)
                );
            }

            final List<Class<?>> typeList = new ArrayList<>();
            //typeList.addAll(element.properties.input1Type.extractTypes());
            //typeList.addAll(element.properties.input2Type.extractTypes());
            //typeList.addAll(element.properties.outputType.extractTypes());

            if (element.properties.functionTypeName != null) {
                try {
                    typeList.add(Class.forName(element.properties.functionTypeName));
                } catch (ClassNotFoundException e) {
                    throw new IllegalStateException("UDF class " + element.properties.functionTypeName + " not found");
                }
            }

            if (element.properties.operatorType == DataflowNodeProperties.DataflowNodeType.IMMUTABLE_DATASET ||
                element.properties.operatorType == DataflowNodeProperties.DataflowNodeType.MUTABLE_DATASET) {

                currentConnector = topologyBuilder.addNode(
                        new Topology.DatasetNode(element.properties),
                        typeList);

            } else {

                currentConnector = topologyBuilder.addNode(
                        new Topology.OperatorNode(element.properties),
                        typeList);
            }

        //}
    }

    public Topology.AuraTopology toTopology(final String name) {
        // sanity check.
        if (name == null)
            throw new IllegalArgumentException("name == null");

        return topologyBuilder.build(name);
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private Topology.Edge.TransferType selectEdgeTransferType(final DataflowNodeProperties srcOperator,
                                                              final DataflowNodeProperties dstOperator) {

        if (ArrayUtils.equals(srcOperator.partitioningKeys, dstOperator.partitioningKeys) // TODO: only subset of keys should be enough.. isPartOf()
                && srcOperator.strategy == dstOperator.strategy) {
            return Topology.Edge.TransferType.POINT_TO_POINT;
        } else {
            return Topology.Edge.TransferType.ALL_TO_ALL;
        }
    }
}
