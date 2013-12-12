package de.tuberlin.aura.demo.rpc;

import java.util.List;
import java.util.UUID;

public interface FooProtocol {

    public Integer foo( String s, Integer q, UUID a, Integer y );

    public void foo1( List<Integer> il );

}
