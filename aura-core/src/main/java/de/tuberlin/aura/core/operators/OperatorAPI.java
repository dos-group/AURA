package de.tuberlin.aura.core.operators;

import java.io.Serializable;

import de.tuberlin.aura.core.common.utils.IVisitable;
import de.tuberlin.aura.core.common.utils.IVisitor;

/**
 *
 */
public final class OperatorAPI {

    // Disallow instantiation.
    private OperatorAPI() {}

    /**
     *
     */
    public static class Operator implements Serializable, IVisitable<Operator> {

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
        public void accept(final IVisitor<Operator> visitor) {
            visitor.visit(this);
        }
    }

    /**
     *
     */
    public static final class PlanPrinter implements IVisitor<Operator> {

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

            for(int i = 0; i < level; ++i) {
                System.out.print("  ");
                System.out.print("|");
            }

            System.out.println();

            for(int i = 0; i < level; ++i) {
                System.out.print("  ");
                System.out.print("|");
            }

            System.out.print("+-" + element.properties.operatorType);
            System.out.print("[");
            System.out.print(element.properties.instanceName);
            System.out.print("]\n");

            if (element.properties.operatorType.operatorInputArity == OperatorProperties.OperatorInputArity.UNARY) {

                level++;
                    visit(element.inputOp1);
                level--;

            } else if (element.properties.operatorType.operatorInputArity == OperatorProperties.OperatorInputArity.BINARY) {

                level++;
                    visit(element.inputOp1);
                    visit(element.inputOp2);
                level--;
            }
        }
    }
}
