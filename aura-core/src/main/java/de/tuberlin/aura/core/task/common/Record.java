package de.tuberlin.aura.core.task.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class Record {
    private Object data;

    private Kryo kryo;

    public Record(Object data) {
        kryo = new Kryo();
        this.data = data;
    }

    public Record(byte[] buffer) {
        kryo = new Kryo();
        Input input = new Input(buffer);
        Class clazz = kryo.readClass(input).getType();
        data = kryo.readObject(input, clazz);
    }

    public Object getData() {
        return data;
    }

    public void serialize(byte[] buffer) {
        Output output = new Output(buffer);
        kryo.writeClassAndObject(output, data);
        output.close();
    }
}

