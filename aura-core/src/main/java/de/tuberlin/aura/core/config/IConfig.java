package de.tuberlin.aura.core.config;

import com.typesafe.config.ConfigObject;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface IConfig {
    boolean hasPath(String path);

    boolean isEmpty();

    Number getNumber(String path);

    boolean getBoolean(String path);

    int getInt(String path);

    long getLong(String path);

    double getDouble(String path);

    String getString(String path);

    ConfigObject getObject(String path);

    Object getAnyRef(String path);

    Long getBytes(String path);

    long getDuration(String path, TimeUnit unit);

    List<Boolean> getBooleanList(String path);

    List<Number> getNumberList(String path);

    List<Integer> getIntList(String path);

    List<Long> getLongList(String path);

    List<Double> getDoubleList(String path);

    List<String> getStringList(String path);

    List<? extends ConfigObject> getObjectList(String path);

    List<? extends Object> getAnyRefList(String path);

    List<Long> getBytesList(String path);

    List<Long> getDurationList(String path, TimeUnit unit);
}
