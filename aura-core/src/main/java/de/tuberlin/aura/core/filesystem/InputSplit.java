package de.tuberlin.aura.core.filesystem;


import java.io.Serializable;

public interface InputSplit extends Serializable {

    public abstract int getSplitNumber();
}
