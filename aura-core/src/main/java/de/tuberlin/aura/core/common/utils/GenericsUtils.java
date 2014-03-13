package de.tuberlin.aura.core.common.utils;


public final class GenericsUtils {

    // Disallow instantiation.
    private GenericsUtils() {
    }

    public static <T> T convertInstanceOfObject(Object obj, Class<T> clazz) {
        // sanity check.
        if (obj == null)
            throw new IllegalArgumentException("obj == null");
        if (clazz == null)
            throw new IllegalArgumentException("clazz == null");
        try {
            return clazz.cast(obj);
        } catch (ClassCastException e) {
            return null; // TODO: exception or null ?
        }
    }
}
