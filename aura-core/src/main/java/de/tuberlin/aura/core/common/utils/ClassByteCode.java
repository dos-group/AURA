package de.tuberlin.aura.core.common.utils;

/**
 *
 */
public class ClassByteCode {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final String className;

    public final byte[] byteCode;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public ClassByteCode(final String className, final byte[] byteCode) {
        // sanity check.
        if (className == null)
            throw new IllegalArgumentException("className == null");
        if (byteCode == null)
            throw new IllegalArgumentException("byteCode == null");

        this.className = className;

        this.byteCode = byteCode;
    }
}
