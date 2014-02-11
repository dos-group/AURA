package de.tuberlin.aura.core.common.utils;

import java.util.ArrayList;
import java.util.List;

import de.tuberlin.aura.core.common.eventsystem.IEventDispatcher;

public class PipelineAssembler {

    /**
     *
     */
    public static final class AssemblyPipeline {

        public AssemblyPipeline(final IEventDispatcher dispatcher) {
            // sanity check.
            if (dispatcher == null)
                throw new IllegalArgumentException("dispatcher == null");

            this.dispatcher = dispatcher;

            this.phases = new ArrayList<AssemblyPhase<Object, Object>>();
        }

        private final IEventDispatcher dispatcher;

        private final List<AssemblyPhase<Object, Object>> phases;

        @SuppressWarnings("unchecked")
        public <I, O> AssemblyPipeline addPhase(final AssemblyPhase<I, O> phase) {
            // sanity check.
            if (phase == null)
                throw new IllegalArgumentException("phase == null");

            phase.setDispatcher(dispatcher);
            phases.add((AssemblyPhase<Object, Object>) phase);
            return this;
        }

        public Object assemble(Object in) {
            // sanity check.
            if (in == null)
                throw new IllegalArgumentException("in == null");

            for (final AssemblyPhase<Object, Object> phase : phases) {
                in = phase.apply(in);
            }

            return in;
        }
    }

    /**
     *
     */
    public static abstract class AssemblyPhase<I, O> {

        protected IEventDispatcher dispatcher;

        public void setDispatcher(final IEventDispatcher dispatcher) {
            // sanity check.
            if (dispatcher == null)
                throw new IllegalArgumentException("dispatcher == null");

            this.dispatcher = dispatcher;
        }

        public abstract O apply(final I in);
    }
}
