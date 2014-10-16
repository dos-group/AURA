package de.tuberlin.aura.core.taskmanager.usercode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.bcel.Repository;
import org.apache.bcel.classfile.*;

import de.tuberlin.aura.core.common.utils.Compression;

public final class UserCodeExtractor {

    // TODO: Look at ShrinkWrap

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private final List<String> standardDependencies;

    private final boolean analyseDependencies;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public UserCodeExtractor(boolean analyseDependencies) {

        this.standardDependencies = new ArrayList<>();

        this.analyseDependencies = analyseDependencies;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public UserCodeExtractor addStandardDependency(final String path) {
        // sanity check.
        if (path == null)
            throw new IllegalArgumentException("path == null");

        this.standardDependencies.add(path);
        return this;
    }

    public UserCode extractUserCodeClass(final Class<?> clazz) {
        // sanity check.
        if (clazz == null)
            throw new IllegalArgumentException("clazz == null");

        if (clazz.isMemberClass() && !Modifier.isStatic(clazz.getModifiers()))
            throw new IllegalStateException();

        final List<String> dependencies;
        if (analyseDependencies)
            dependencies = buildTransitiveDependencyClosure(clazz, new ArrayList<String>());
        else
            dependencies = new ArrayList<>();

        return new UserCode(clazz.getName(), clazz.getSimpleName(), dependencies, Compression.compress(loadByteCode(clazz)));
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private List<String> buildTransitiveDependencyClosure(final Class<?> clazz, final List<String> globalDependencies) {

        final String fullQualifiedPath = clazz.getCanonicalName();
        final List<String> levelDependencies = DependencyEmitter.analyze(clazz);
        for (String dependency : levelDependencies) {

            boolean isNewDependency = true;
            for (final String sd : standardDependencies)
                isNewDependency &= !dependency.contains(sd);

            if (isNewDependency) {

                final String dp1 = dependency.replace("/", ".");
                final String dp2 = dp1.replace("$", ".");

                boolean isTransitiveEnclosingClass = false;
                for (final String dp : globalDependencies)
                    if (dp.contains(dp2)) {
                        isTransitiveEnclosingClass = true;
                        break;
                    }

                if (!fullQualifiedPath.contains(dp2) && !isTransitiveEnclosingClass) {
                    globalDependencies.add(dp2);

                    final Class<?> dependencyClass;
                    try {
                        dependencyClass = Class.forName(dp1, false, clazz.getClassLoader());
                    } catch (ClassNotFoundException e) {
                        throw new IllegalStateException(e);
                    }

                    if (!dependencyClass.isArray() && !dependencyClass.isPrimitive())
                        buildTransitiveDependencyClosure(dependencyClass, globalDependencies);
                }
            }
        }

        return globalDependencies;
    }

    private byte[] loadByteCode(final Class<?> clazz) {

        // TODO: a simpler way possible!!

        // TODO: handle JAR Files!!

        String topLevelClazzName = null;
        Class<?> enclosingClazz = clazz.getEnclosingClass();
        while (enclosingClazz != null) {
            topLevelClazzName = enclosingClazz.getSimpleName();
            enclosingClazz = enclosingClazz.getEnclosingClass();
        }
        if (topLevelClazzName == null) {
            topLevelClazzName = clazz.getSimpleName();
        }

        final StringTokenizer tokenizer = new StringTokenizer(clazz.getCanonicalName(), ".");
        final StringBuilder pathBuilder = new StringBuilder();

        boolean isClazzFilename = false;
        while (tokenizer.hasMoreTokens()) {
            final String token = tokenizer.nextToken();
            if (!token.equals(topLevelClazzName) && !isClazzFilename)
                pathBuilder.append(token).append("/");
            else {
                pathBuilder.append(token);
                if (tokenizer.hasMoreTokens())
                    pathBuilder.append("$");
                isClazzFilename = true;
            }
        }

        /*
         * int count = 0; boolean isClazzFilename = false; while (tokenizer.hasMoreTokens()) { final
         * String token = tokenizer.nextToken(); if (!token.equals(topLevelClazzName) &&
         * !isClazzFilename && tokenCount - 1 > count) pathBuilder.append(token).append("/"); else {
         * pathBuilder.append(token); if (tokenizer.hasMoreTokens()) pathBuilder.append("$");
         * isClazzFilename = true; } ++count; }
         */

        final String filePath = clazz.getProtectionDomain().getCodeSource().getLocation().getPath() + pathBuilder.toString() + ".class";

        final File clazzFile = new File(filePath.replace("%20", " "));

        FileInputStream fis = null;
        byte[] clazzData = null;
        try {
            fis = new FileInputStream(clazzFile);
            clazzData = new byte[(int) clazzFile.length()]; // TODO: use IOUtils, Apache Commons!
            fis.read(clazzData);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            try {
                if (fis != null)
                    fis.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        return clazzData;
    }

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private static final class DependencyEmitter extends EmptyVisitor {

        public DependencyEmitter(final JavaClass javaClass) {

            this.javaClass = javaClass;

            this.dependencies = new ArrayList<>();
        }

        private final JavaClass javaClass;

        private final List<String> dependencies;

        @Override
        public void visitConstantClass(final ConstantClass obj) {
            final ConstantPool cp = javaClass.getConstantPool();
            String bytes = obj.getBytes(cp);
            dependencies.add(bytes);
        }

        public static List<String> analyze(final Class<?> clazz) {
            final JavaClass javaClass = Repository.lookupClass(clazz);
            final DependencyEmitter visitor = new DependencyEmitter(javaClass);
            (new DescendingVisitor(javaClass, visitor)).visit();
            return visitor.dependencies;
        }
    }
}
