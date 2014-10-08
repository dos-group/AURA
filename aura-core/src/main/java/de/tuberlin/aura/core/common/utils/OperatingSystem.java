package de.tuberlin.aura.core.common.utils;

public enum OperatingSystem {

    LINUX,

    WINDOWS,

    MAC_OS,

    FREE_BSD,

    UNKNOWN;

    public static OperatingSystem getCurrentOperatingSystem() {
        return os;
    }

    public static boolean isWindows() {
        return getCurrentOperatingSystem() == WINDOWS;
    }

    public static boolean isLinux() {
        return getCurrentOperatingSystem() == LINUX;
    }

    public static boolean isMac() {
        return getCurrentOperatingSystem() == MAC_OS;
    }

    public static boolean isFreeBSD() {
        return getCurrentOperatingSystem() == FREE_BSD;
    }

    private static final OperatingSystem os = readOSFromSystemProperties();

    private static OperatingSystem readOSFromSystemProperties() {
        String osName = System.getProperty(OS_KEY);

        if (osName.startsWith(LINUX_OS_PREFIX))
            return LINUX;
        if (osName.startsWith(WINDOWS_OS_PREFIX))
            return WINDOWS;
        if (osName.startsWith(MAC_OS_PREFIX))
            return MAC_OS;
        if (osName.startsWith(FREEBSD_OS_PREFIX))
            return FREE_BSD;

        return UNKNOWN;
    }

    private static final String OS_KEY = "os.name";

    private static final String LINUX_OS_PREFIX = "Linux";

    private static final String WINDOWS_OS_PREFIX = "Windows";

    private static final String MAC_OS_PREFIX = "Mac";

    private static final String FREEBSD_OS_PREFIX = "FreeBSD";
}