package de.tuberlin.aura.core.config;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;

/**
 * An {@code IConfig} delegate for a {@code com.typesafe.config.Config} instance.
 */
public class TypesafeConfig implements IConfig {

    private final Config delegate;

    protected TypesafeConfig(final Config config) {
        this.delegate = config;
    }

    @Override
    public boolean hasPath(String path) {
        return delegate.hasPath(path);
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public Number getNumber(String path) {
        return delegate.getNumber(path);
    }

    @Override
    public boolean getBoolean(String path) {
        return delegate.getBoolean(path);
    }

    @Override
    public int getInt(String path) {
        return delegate.getInt(path);
    }

    @Override
    public long getLong(String path) {
        return delegate.getLong(path);
    }

    @Override
    public double getDouble(String path) {
        return delegate.getDouble(path);
    }

    @Override
    public String getString(String path) {
        return delegate.getString(path);
    }

    @Override
    public IConfig getObject(String path) {
        return new TypesafeConfig(delegate.getObject(path).toConfig());
    }

    @Override
    public IConfig getConfig(String path) {
        return new TypesafeConfig(delegate.getConfig(path));
    }

    @Override
    public Object getAnyRef(String path) {
        return delegate.getAnyRef(path);
    }

    @Override
    public Long getBytes(String path) {
        return delegate.getBytes(path);
    }

    @Override
    public long getDuration(String path, TimeUnit unit) {
        return delegate.getDuration(path, unit);
    }

    @Override
    public List<Boolean> getBooleanList(String path) {
        return delegate.getBooleanList(path);
    }

    @Override
    public List<Number> getNumberList(String path) {
        return delegate.getNumberList(path);
    }

    @Override
    public List<Integer> getIntList(String path) {
        return delegate.getIntList(path);
    }

    @Override
    public List<Long> getLongList(String path) {
        return delegate.getLongList(path);
    }

    @Override
    public List<Double> getDoubleList(String path) {
        return delegate.getDoubleList(path);
    }

    @Override
    public List<String> getStringList(String path) {
        return delegate.getStringList(path);
    }

    @Override
    public List<? extends IConfig> getObjectList(String path) {
        List<? extends ConfigObject> x = delegate.getObjectList(path);
        List<IConfig> y = new ArrayList<IConfig>(x.size());
        for (ConfigObject o : x) {
            y.add(new TypesafeConfig(o.toConfig()));
        }
        return y;
    }

    @Override
    public List<?> getAnyRefList(String path) {
        return delegate.getAnyRefList(path);
    }

    @Override
    public List<Long> getBytesList(String path) {
        return delegate.getBytesList(path);
    }

    @Override
    public List<Long> getDurationList(String path, TimeUnit unit) {
        return delegate.getDurationList(path, unit);
    }
}
