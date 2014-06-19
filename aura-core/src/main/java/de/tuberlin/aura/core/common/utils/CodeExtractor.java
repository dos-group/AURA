package de.tuberlin.aura.core.common.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 *
 */
public final class CodeExtractor {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static byte[] loadByteCode(final Class<?> clazz) {

        // TODO: a simpler way possible!!

        // TODO: handle JAR Files!!

        String topLevelClazzName = null;
        Class<?> enclosingClazz = clazz.getEnclosingClass();
        while (enclosingClazz != null) {
            topLevelClazzName = enclosingClazz.getSimpleName();
            enclosingClazz = enclosingClazz.getEnclosingClass();
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

        final String filePath = clazz.getProtectionDomain().getCodeSource().getLocation().getPath() + pathBuilder.toString() + ".class";

        final File clazzFile = new File(filePath.replace("%20", " "));

        FileInputStream fis = null;
        byte[] clazzData = null;
        try {
            fis = new FileInputStream(clazzFile);
            clazzData = new byte[(int) clazzFile.length()]; // TODO: use IOUtils, Apache Commons!
            fis.read(clazzData);
        } catch (FileNotFoundException e) {
            throw new IllegalStateException(e);
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
}
