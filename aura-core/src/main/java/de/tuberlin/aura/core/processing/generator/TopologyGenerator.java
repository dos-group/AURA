package de.tuberlin.aura.core.processing.generator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import de.tuberlin.aura.core.common.utils.ArrayUtils;
import de.tuberlin.aura.core.common.utils.IVisitor;
import de.tuberlin.aura.core.processing.api.OperatorAPI;
import de.tuberlin.aura.core.processing.api.OperatorProperties;
import de.tuberlin.aura.core.topology.Topology;

/**
 *
 */
public final class TopologyGenerator implements IVisitor<OperatorAPI.Operator> {

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

            final List<Class<?>> typeList = new ArrayList<>();
            //typeList.addAll(element.properties.outputType.extractTypes());

            if (element.properties.function != null) {
                typeList.add(element.properties.function);
            }

            currentConnector = topologyBuilder.addNode(
                    new Topology.OperatorNode(element.properties),
                    typeList
            );

        } else if (element.properties.operatorType.operatorInputArity == OperatorProperties.OperatorInputArity.UNARY) {

            visit(element.inputOp1);

            currentConnector.connectTo(
                    element.properties.instanceName,
                    selectEdgeTransferType(element.inputOp1.properties, element.properties)
            );

            final List<Class<?>> typeList = new ArrayList<>();
            //typeList.addAll(element.properties.input1Type.extractTypes());
            //typeList.addAll(element.properties.outputType.extractTypes());

            if (element.properties.function != null) {
                typeList.add(element.properties.function);
            }

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

            final List<Class<?>> typeList = new ArrayList<>();
            //typeList.addAll(element.properties.input1Type.extractTypes());
            //typeList.addAll(element.properties.input2Type.extractTypes());
            //typeList.addAll(element.properties.outputType.extractTypes());

            if (element.properties.function != null) {
                typeList.add(element.properties.function);
            }

            currentConnector = topologyBuilder.addNode(
                    new Topology.OperatorNode(element.properties),
                    typeList
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

        if (ArrayUtils.equals(srcOperator.partitioningKeys, dstOperator.partitioningKeys) // TODO: only subset of keys should be enough.. isPartOf()
                && srcOperator.strategy == dstOperator.strategy) {
            return Topology.Edge.TransferType.POINT_TO_POINT;
        } else {
            return Topology.Edge.TransferType.ALL_TO_ALL;
        }
    }
}
