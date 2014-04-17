package de.tuberlin.aura.core.task.common;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.tuberlin.aura.core.iosystem.UUIDSerializer;
import de.tuberlin.aura.core.memory.MemoryManager;

public class Record<T> {

    private static final Logger LOG = LoggerFactory.getLogger(Record.class);

    // TODO: Using one Kryo object for all Records, causes IndexOutOfBoundsExceptions... figure out
    // why
    // private static final Kryo KRYO;
    //
    // static {
    // KRYO = new Kryo();
    // KRYO.addDefaultSerializer(UUID.class, new UUIDSerializer());
    // }

    private T data;

    private Kryo kryo;

    public Record(T data) {
        this.data = data;
        this.kryo = new Kryo();
        this.kryo.addDefaultSerializer(UUID.class, new UUIDSerializer());
    }

    public Record(MemoryManager.MemoryView view) {
        Input input = new Input(view.memory);
        input.skip(view.baseOffset);

        this.kryo = new Kryo();
        this.kryo.addDefaultSerializer(UUID.class, new UUIDSerializer());

        this.data = (T) kryo.readClassAndObject(input);
    }

    public T getData() {
        return this.data;
    }

    public void serialize(MemoryManager.MemoryView view) {

        Output output = new Output(view.memory);
        output.setPosition(view.baseOffset);

        this.kryo.writeClassAndObject(output, this.data);

        output.close();
    }
}
