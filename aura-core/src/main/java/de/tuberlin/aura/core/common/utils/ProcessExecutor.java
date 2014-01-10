package de.tuberlin.aura.core.common.utils;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ProcessExecutor {

	// ---------------------------------------------------
	// Constructors.
	// ---------------------------------------------------

	public ProcessExecutor(final Class<?> executeableClazz) {
		// sanity check.
		if (executeableClazz == null)
			throw new IllegalArgumentException("executeableClazz == null");

		try {
			executeableClazz.getMethod("main", String[].class);
		} catch (NoSuchMethodException | SecurityException e) {
			throw new IllegalArgumentException("executeableClazz has no accessible main method", e);
		}

		this.executeableClazz = executeableClazz;

		this.process = null;
	}

	// ---------------------------------------------------
	// Fields.
	// ---------------------------------------------------

	private final Class<?> executeableClazz;

	private Process process;

	// ---------------------------------------------------
	// Public.
	// ---------------------------------------------------

	public ProcessExecutor execute(String... params) {

		final String javaRuntime = System.getProperty("java.home") + "/bin/java";
		final String classpath = System.getProperty("java.class.path") + ":"
			+ executeableClazz.getProtectionDomain().getCodeSource().getLocation().getPath();
		final String canonicalName = executeableClazz.getCanonicalName();

		try {
			final List<String> commandList = new ArrayList<String>();
			commandList.add(javaRuntime);
			commandList.add("-cp");
			commandList.add(classpath);
			commandList.add(canonicalName);
			commandList.addAll(Arrays.asList(params));

			final ProcessBuilder builder = new ProcessBuilder(commandList);
			builder.redirectErrorStream(true);
			builder.redirectOutput(Redirect.INHERIT);
			process = builder.start();
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
		return this;
	}

	public void destroy() {
		if (process == null)
			throw new IllegalStateException("process == null");
		process.destroy();
	}
}
