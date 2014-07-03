package de.tuberlin.aura.core.operators;

import java.io.Serializable;

import de.tuberlin.aura.core.common.utils.Visitable;
import de.tuberlin.aura.core.common.utils.Visitor;

/**
 *
 */
public final class OperatorAPI {

    // Disallow instantiation.
    private OperatorAPI() {}

    /**
     *
     */
    public static class Operator implements Serializable, Visitable<Operator> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        public final OperatorProperties properties;

        public final Operator inputOp1;

        public final Operator inputOp2;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        public Operator(final OperatorProperties properties, final Operator inputOp1, final Operator inputOp2) {

            this.properties = properties;

            this.inputOp1 = inputOp1;

            this.inputOp2 = inputOp2;
        }

        public Operator(final OperatorProperties properties, final Operator inputOp1) {
            this(properties, inputOp1, null);
        }

        public Operator(final OperatorProperties properties) {
            this(properties, null, null);
        }

        @Override
        public void accept(final Visitor<Operator> visitor) {
            visitor.visit(this);
        }
    }

    /**
     *
     */
    public static final class PlanPrinter implements Visitor<OperatorAPI.Operator> {

        // ---------------------------------------------------
        // Fields.
        // ---------------------------------------------------

        //private int indentation = 0;

        // ---------------------------------------------------
        // Constructor.
        // ---------------------------------------------------

        private PlanPrinter() {
        }

        // ---------------------------------------------------
        // Public Static Methods.
        // ---------------------------------------------------

        public static void printPlan(final OperatorAPI.Operator element) {
            // sanity check.
            if (element == null)
                throw new IllegalArgumentException("element == null");

            element.accept(new PlanPrinter());
        }

        // ---------------------------------------------------
        // Public Methods.
        // ---------------------------------------------------

        @Override
        public void visit(final OperatorAPI.Operator element) {

            if (element.properties.operatorType.operatorInputArity == OperatorProperties.OperatorInputArity.UNARY) {

                visit(element.inputOp1);

            } else if (element.properties.operatorType.operatorInputArity == OperatorProperties.OperatorInputArity.BINARY) {

                visit(element.inputOp1);

                visit(element.inputOp2);
            }

            System.out.print(element.properties.operatorType + "(" + element.properties.instanceName + ")\n");
        }
    }
}
