package de.tuberlin.aura.core.operators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import de.tuberlin.aura.core.common.utils.Visitor;
import de.tuberlin.aura.core.topology.Topology;

/**
 *
 */
public final class TopologyGenerator implements Visitor<OperatorAPI.Operator> {

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

    public TopologyGenerator generate(final OperatorAPI.Operator element) {
        element.accept(this);
        return this;
    }

    @Override
    public void visit(final OperatorAPI.Operator element) {

        if (element.properties.operatorType.operatorInputArity == OperatorProperties.OperatorInputArity.NULLARY) {

            currentConnector = topologyBuilder.addNode(
                    new Topology.OperatorNode(element.properties),
                    element.properties.outputType
            );

        } else if (element.properties.operatorType.operatorInputArity == OperatorProperties.OperatorInputArity.UNARY) {

            visit(element.inputOp1);

            currentConnector.connectTo(
                    element.properties.instanceName,
                    selectEdgeTransferType(element.inputOp1.properties, element.properties)
            );

            final List<Class<?>> typeList = new ArrayList<>();

            if(element.properties.input1Type != null)
                typeList.add(element.properties.input1Type);
            if(element.properties.outputType != null)
                typeList.add(element.properties.outputType);

            currentConnector = topologyBuilder.addNode(
                    new Topology.OperatorNode(element.properties),
                    typeList
            );

        } else if (element.properties.operatorType.operatorInputArity == OperatorProperties.OperatorInputArity.BINARY) {

            visit(element.inputOp1);

            currentConnector.connectTo(
                    element.properties.instanceName,
                    selectEdgeTransferType(element.inputOp1.properties, element.properties)
            );

            visit(element.inputOp2);

            currentConnector.connectTo(
                    element.properties.instanceName,
                    selectEdgeTransferType(element.inputOp2.properties, element.properties)
            );

            currentConnector = topologyBuilder.addNode(
                    new Topology.OperatorNode(element.properties),
                    Arrays.asList(
                            element.properties.input1Type,
                            element.properties.input1Type,
                            element.properties.outputType
                    )
            );
        }
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

    private Topology.Edge.TransferType selectEdgeTransferType(final OperatorProperties srcOperator,
                                                               final OperatorProperties dstOperator) {

        if (Arrays.equals(srcOperator.keys, dstOperator.keys) // TODO: only subset of keys should be enough.. isPartOf()
                && srcOperator.strategy == dstOperator.strategy) {
            return Topology.Edge.TransferType.POINT_TO_POINT;
        } else {
            return Topology.Edge.TransferType.ALL_TO_ALL;
        }
    }
}
