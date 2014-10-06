package de.tuberlin.aura.core.record;

public class OperatorResult<T> {

    public enum StreamMarker {

        END_OF_STREAM_MARKER,

        END_OF_GROUP_MARKER,

        END_OF_ITERATION_MARKER,

        START_OF_ITERATION_MARKER
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public T element;

    public StreamMarker marker;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public OperatorResult(T element, StreamMarker marker) {

        this.element = element;

        this.marker = marker;
    }
}
