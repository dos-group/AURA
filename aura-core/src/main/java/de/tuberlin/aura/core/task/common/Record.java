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

    private T data;

    private Kryo kryo;

    public Record(T data) {
        this.data = data;
        this.kryo = new Kryo();
        this.kryo.addDefaultSerializer(UUID.class, new UUIDSerializer());
    }

    public Record(MemoryManager.MemoryView view) {
        this.kryo = new Kryo();
        this.kryo.addDefaultSerializer(UUID.class, new UUIDSerializer());

        Input input = new Input(view.memory);
        input.skip(view.baseOffset);

        // Registration reg = this.kryo.readClass(input);
        // if (reg == null) {
        // try {
        // LOG.error(Integer.toString(input.available()));
        // } catch (IOException e) {
        // e.printStackTrace();
        // }
        // }
        //
        // Class<T> clazz = (Class<T>) reg.getType();

        this.data = (T) this.kryo.readClassAndObject(input);
        // LOG.debug("Read: " + Integer.toString(input.position()) + "/" +
        // Integer.toString(buffer.length));
        // if (input.position() != 58) {
        // LOG.error("" + this.data);
        // }
        // this.data = this.kryo.readObject(input, clazz);
    }

    public T getData() {
        return this.data;
    }

    public void serialize(MemoryManager.MemoryView view) {

        Output output = new Output(view.memory);
        output.setPosition(view.baseOffset);

        this.kryo.writeClassAndObject(output, this.data);
        // LOG.debug("Written: " + Integer.toString(output.position()) + "/" +
        // Integer.toString(buffer.length));
        // if (output.position() != 58) {
        // LOG.error(this.data.toString());
        // }

        output.close();
    }
}
