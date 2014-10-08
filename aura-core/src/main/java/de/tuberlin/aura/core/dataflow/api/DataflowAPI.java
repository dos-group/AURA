package de.tuberlin.aura.core.dataflow.api;

import java.io.Serializable;

import de.tuberlin.aura.core.common.utils.IVisitable;
import de.tuberlin.aura.core.common.utils.IVisitor;

//@Deprecated
public final class DataflowAPI {

    // Disallow instantiation.
    private DataflowAPI() {}

    public static final class DataflowNodeDescriptor implements Serializable, IVisitable<DataflowNodeDescriptor> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        public final DataflowNodeProperties properties;

        public final DataflowNodeDescriptor input1;

        public final DataflowNodeDescriptor input2;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        public DataflowNodeDescriptor(final DataflowNodeProperties properties,
                                      final DataflowNodeDescriptor input1,
                                      final DataflowNodeDescriptor input2) {

            this.properties = properties;

            this.input1 = input1;

            this.input2 = input2;
        }

        public DataflowNodeDescriptor(final DataflowNodeProperties properties,
                                      final DataflowNodeDescriptor input1) {
            this(properties, input1, null);
        }

        public DataflowNodeDescriptor(final DataflowNodeProperties properties) {
            this(properties, null, null);
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void accept(final IVisitor<DataflowNodeDescriptor> visitor) {
            visitor.visit(this);
        }
    }


    public static final class PlanPrinter implements IVisitor<DataflowNodeDescriptor> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        private int level = 0;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        private PlanPrinter() {
        }

        // ---------------------------------------------------
        // Public Static Methods.
        // ---------------------------------------------------

        public static void printPlan(final DataflowNodeDescriptor element) {
            // sanity check.
            if (element == null)
                throw new IllegalArgumentException("element == null");

            element.accept(new PlanPrinter());
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void visit(final DataflowNodeDescriptor element) {

            for(int i = 0; i < level; ++i) {
                System.out.print("  ");
                System.out.print("|");
            }

            System.out.println();

            for(int i = 0; i < level; ++i) {
                System.out.print("  ");
                System.out.print("|");
            }

            System.out.print("+-" + element.properties.type);
            System.out.print("[");
            System.out.print(element.properties.instanceName);
            System.out.print("]\n");

            if (element.properties.type.operatorInputArity == DataflowNodeProperties.InputArity.UNARY) {

                level++;
                    visit(element.input1);
                level--;

            } else if (element.properties.type.operatorInputArity == DataflowNodeProperties.InputArity.BINARY) {

                level++;
                    visit(element.input1);
                    visit(element.input2);
                level--;
            }
        }
    }
}
