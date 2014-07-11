package de.tuberlin.aura.core.config;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface IConfig {

    public static enum Type {
        WM("wm"),
        TM("tm"),
        CLIENT("client"),
        SIMULATOR("simulator");

        protected final String name;

        private Type(String name) {
            this.name = name;
        }
    }

    boolean hasPath(String path);

    boolean isEmpty();

    Number getNumber(String path);

    boolean getBoolean(String path);

    int getInt(String path);

    long getLong(String path);

    double getDouble(String path);

    String getString(String path);

    IConfig getObject(String path);

    IConfig getConfig(String path);

    Object getAnyRef(String path);

    Long getBytes(String path);

    long getDuration(String path, TimeUnit unit);

    List<Boolean> getBooleanList(String path);

    List<Number> getNumberList(String path);

    List<Integer> getIntList(String path);

    List<Long> getLongList(String path);

    List<Double> getDoubleList(String path);

    List<String> getStringList(String path);

    List<? extends IConfig> getObjectList(String path);

    List<? extends Object> getAnyRefList(String path);

    List<Long> getBytesList(String path);

    List<Long> getDurationList(String path, TimeUnit unit);
}
