package de.tuberlin.aura.core.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import de.tuberlin.aura.core.common.utils.InetHelper;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class IConfigFactory {

    /**
     * Factory method.
     * 
     * @see IConfigFactory#load(de.tuberlin.aura.core.config.IConfig.Type)
     * @return
     */
    public static IConfig load() {
        return load(null);
    }

    /**
     * Factory method.
     * 
     * This will construct an resolve a TypesafeConfig delegate through a merge of the following
     * configuration layers (higher number in the list means higher priority):
     * 
     * <ol>
     * <li/>A classpath entry named <code>reference.${type}.conf</code> containing default values.
     * <li/>A config containing values from the current runtime (e.g. the number of CPU cores).
     * <li/>A classpath entry named <code>profile.${aura.profile}.conf</code> (optional).
     * <li/>A user-provided file residing under <code>${aura.path.config}/aura.${type}.conf</code>.
     * <li/>A config constructed from the current System properties.
     * </ol>
     * 
     * @param type The configuration type to load. If <code>null</code>, only the base config is
     *        loaded.
     * @return A resolved config instance constructed according to the above guidelines.
     */
    public static IConfig load(IConfig.Type type) {
        // options for configuration parsing
        ConfigParseOptions opts = ConfigParseOptions.defaults().setClassLoader(TypesafeConfig.class.getClassLoader());

        // type suffix for the files at step 1 and 4
        String s = type != null ? String.format("%s.conf", type.name) : "conf";

        // 0. initial configuration is empty
        Config config = ConfigFactory.empty();
        // 1. merge a config from a classpath entry named 'reference.conf'
        config = ConfigFactory.parseResources(String.format("reference.%s", s), opts).withFallback(config);
        // 2. merge a config constructed from current system data
        config = currentRuntimeConfig().withFallback(config);
        // 3. merge a config from a classpath entry named '${aura.profile}.profile.conf'
        if (System.getProperty("aura.profile") != null)
            config = ConfigFactory.parseResources(String.format("profile.%s.conf", System.getProperty("aura.profile")), opts).withFallback(config);
        // 4. merge a config from a file named ${aura.path.config}/aura.conf
        config = ConfigFactory.parseFile(new File(String.format("%s/aura.%s", System.getProperty("aura.path.config"), s)), opts).withFallback(config);
        // 5. merge system properties as configuration
        config = ConfigFactory.systemProperties().withFallback(config);

        // wrap the resolved delegate into a TypesafeConfig new instance and return
        return new TypesafeConfig(config.resolve());
    }

    /**
     * Loads default values from the current runtime config.
     * 
     * @return
     */
    private static Config currentRuntimeConfig() {
        // initial empty configuration
        Map<String, Object> runtimeConfig = new HashMap<>();

        runtimeConfig.put("default.machine.cpu.cores", Runtime.getRuntime().availableProcessors());
        runtimeConfig.put("default.machine.memory.max", Runtime.getRuntime().maxMemory());
        runtimeConfig.put("default.machine.disk.size", new File("/").getTotalSpace()); // FIXME
        runtimeConfig.put("default.io.tcp.port", InetHelper.getFreePort());
        runtimeConfig.put("default.io.rpc.port", InetHelper.getFreePort());

        return ConfigFactory.parseMap(runtimeConfig);
    }
}
