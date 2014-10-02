package de.tuberlin.aura.core.record;

public class OperatorResult<T> {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    final private T element;

    final public boolean isEndOfStream;

    final public boolean isEndOfGroup;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public OperatorResult(T result) {
        this(result, false, false);
    }

    public OperatorResult(T element, boolean isEndOfGroup, boolean isEndOfData) {
        this.element = element;
        this.isEndOfGroup = isEndOfGroup;
        this.isEndOfStream = isEndOfData;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static OperatorResult endOfStream() {
        return new OperatorResult<>(null, false, true);
    }

    public static OperatorResult endOfGroup() {
        return new OperatorResult<>(null, true, false);
    }

}
