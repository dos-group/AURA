package de.tuberlin.aura.core.task.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class Record<T> {

    private T data;

    private Kryo kryo = new Kryo();

    public Record(T data) {
        this.data = data;
    }

    public Record(byte[] buffer) {
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

