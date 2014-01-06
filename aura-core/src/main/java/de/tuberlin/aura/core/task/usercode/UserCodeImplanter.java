package de.tuberlin.aura.core.task.usercode;

import de.tuberlin.aura.core.common.utils.Compression;

public final class UserCodeImplanter {

	// ---------------------------------------------------
	// Inner Classes.
	// ---------------------------------------------------

	/**
     *
     */
	private static final class DynamicClassLoader extends ClassLoader {

		public DynamicClassLoader(final ClassLoader cl) {
			super(cl);
		}

		public Class<?> buildClassFromByteArray(final String clazzName, final byte[] clazzData) {
			// sanity check.
			if (clazzName == null)
				throw new IllegalArgumentException("clazzName == null");
			if (clazzData == null)
				throw new IllegalArgumentException("clazzData == null");

			Class<?> newClazz = null;
			try {
				newClazz = this.defineClass(clazzName, clazzData, 0, clazzData.length);
			} catch (ClassFormatError e) {
				throw new IllegalStateException(e);
			}

			return newClazz;
		}
	}

	// ---------------------------------------------------
	// Constructors.
	// ---------------------------------------------------

	public UserCodeImplanter(final ClassLoader cl) {
		// sanity check.
		if (cl == null)
			throw new IllegalArgumentException("cl == null");

		this.classLoader = new DynamicClassLoader(cl);
	}

	// ---------------------------------------------------
	// Fields.
	// ---------------------------------------------------

	private final DynamicClassLoader classLoader;

	// ---------------------------------------------------
	// Public.
	// ---------------------------------------------------

	public Class<?> implantUserCodeClass(final UserCode userCode) {
		// sanity check.
		if (userCode == null)
			throw new IllegalArgumentException("userCode == null");

		try {
			for (final String dependency : userCode.classDependencies) {
				Class.forName(dependency, false, classLoader);
			}
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(e);
		}

		Class<?> clazz;
		try {
			clazz = Class.forName(userCode.className);
		} catch (ClassNotFoundException e) {
			clazz = classLoader.buildClassFromByteArray(userCode.className,
				Compression.decompress(userCode.classByteCode));
		}

		return clazz;
	}
}
