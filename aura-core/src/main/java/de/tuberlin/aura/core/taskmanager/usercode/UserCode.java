package de.tuberlin.aura.core.taskmanager.usercode;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public final class UserCode implements Serializable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final long serialVersionUID = -1L;

    public final String className;

    public final String simpleClassName;

    public final List<String> classDependencies;

    public final byte[] classByteCode;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public UserCode(final String className, final String simpleClassName, final List<String> classDependencies, final byte[] byteCode) {
        // sanity check.
        if (className == null)
            throw new IllegalArgumentException("className == null");
        if (simpleClassName == null)
            throw new IllegalArgumentException("simpleClassName == null");
        if (classDependencies == null)
            throw new IllegalArgumentException("classDependencies == null");
        if (byteCode == null)
            throw new IllegalArgumentException("byteCode == null");

        this.className = className;

        this.simpleClassName = simpleClassName;

        this.classDependencies = Collections.unmodifiableList(classDependencies);

        this.classByteCode = byteCode;
    }
}
