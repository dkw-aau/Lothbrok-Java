package org.piqnic.piqnic.sparql;

import org.apache.jena.sparql.engine.binding.Binding;

import java.util.ArrayList;
import java.util.List;

public class PiqnicBindings {
    private List<Binding> bindings = new ArrayList<>();

    public void add(Binding binding) {
        bindings.add(binding);
    }

    public Binding get(int i) {
        return bindings.get(i);
    }

    public int size() {
        return bindings.size();
    }

    @Override
    public String toString() {
        return "PiqnicBindings{" +
                "bindings=" + bindings +
                '}';
    }
}
