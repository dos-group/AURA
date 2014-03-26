package de.tuberlin.aura.core.task.common;

import java.util.UUID;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.tuberlin.aura.core.iosystem.UUIDSerializer;

public class Record<T> {

    private T data;

    private Kryo kryo;

    public Record(T data) {
        this.data = data;
        this.kryo = new Kryo();
        this.kryo.addDefaultSerializer(UUID.class, new UUIDSerializer());
    }

    public Record(byte[] buffer) {
        this.kryo = new Kryo();
        this.kryo.addDefaultSerializer(UUID.class, new UUIDSerializer());

        Input input = new Input(buffer);
        Class<T> clazz = (Class<T>) this.kryo.readClass(input).getType();
        this.data = this.kryo.readObject(input, clazz);
    }

    public T getData() {
        return this.data;
    }

    public void serialize(byte[] buffer) {

        Output output = new Output(buffer);

        this.kryo.writeClassAndObject(output, this.data);
        output.close();
    }
}
