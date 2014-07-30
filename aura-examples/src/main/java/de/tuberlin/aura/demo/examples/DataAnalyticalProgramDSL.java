package de.tuberlin.aura.demo.examples;


public final class DataAnalyticalProgramDSL {

    public static final class Program {

        private Program() {
        }

        public static Program create() {
            return new Program();
        }


        public IfControlFlow _if(boolean predicate) {
            return new IfControlFlow(this);
        }

        public IfControlFlow _if(final DataFlow df) {
            return new IfControlFlow(this);
        }


        public WhileControlFlow _while(boolean predicate) {
            return new WhileControlFlow(this);
        }

        public WhileControlFlow _while(final DataFlow df) {
            return new WhileControlFlow(this);
        }


        public Program _call(final Program program) {
            return this;
        }


        public Program _call(final DataFlow df) {
            return this;
        }

        public Program _call(final DataSet ds) {
            return this;
        }


        public static final class WhileControlFlow {


            private final Program envProgram;


            public WhileControlFlow(final Program envProgram) {
                this.envProgram = envProgram;
            }


            public Program _body(final Program program) {
                return envProgram;
            }


            public Program _body(final DataFlow df) {
                return envProgram;
            }


            public Program _body(final DataSet ds) {
                return envProgram;
            }
        }


        public static final class IfControlFlow {


            private final Program envProgram;


            public IfControlFlow(final Program envProgram) {
                this.envProgram = envProgram;
            }


            public Program _body(final Program program) {
                return envProgram;
            }

            public IfControlFlow _body1(final Program program) {
                return this;
            }

            public IfControlFlow _body1(final DataFlow df) {
                return this;
            }


            public IfControlFlow _body1(final DataSet ds) {
                return this;
            }


            public Program _else(final Program program) {
                return envProgram;
            }


            public Program _else(final DataFlow df) {
                return envProgram;
            }


            public Program _else(final DataSet ds) {
                return envProgram;
            }
        }
    }


    public static final class DataFlow {


        private DataFlow() {
        }

        public static DataFlow source(final String name) {
            return new DataFlow();
        }


        public static DataFlow source(final DataSet ds) {
            return new DataFlow();
        }


        public static DataFlow source() {
            return new DataFlow();
        }


        public DataFlow map() {
            return this;
        }


        public DataFlow join(final DataFlow df) {
            return this;
        }


        public DataFlow filter() {
            return this;
        }


        public DataFlow union(final DataFlow df) {
            return this;
        }


        public DataSet produce(final String name) {
            return new DataSet();
        }
    }


    public static final class DataSet {


        public static DataSet get(final String name) {
            return new DataSet();
        }


        public DataSet sort() {
            return this;
        }


        public DataSet groupBy() {
            return this;
        }


        public int agg() {
            return 0;
        }


        public DataSet transform(final DataFlow df) {
            return this;
        }
    }


    public static void main(String[] argv) {


        Program.create()
                ._call(
                        DataFlow.source().map().produce("ds1")
                );

        Program.create()
                ._if (DataSet.get("ds1").agg() > 0)
                ._body1(
                        Program.create()
                                ._while(DataSet.get("ds1").agg() > 0)
                                ._body(
                                        DataSet.get("ds1")
                                                .transform(
                                                        DataFlow.source().map()
                                                )
                                )
                )._else(
                    DataFlow.source("ds1").map()
                );
    }
}
