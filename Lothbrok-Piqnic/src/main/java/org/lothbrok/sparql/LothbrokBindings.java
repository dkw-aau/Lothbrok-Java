package org.lothbrok.sparql;

import org.apache.jena.sparql.engine.binding.Binding;

import java.util.ArrayList;
import java.util.List;

public class LothbrokBindings {
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

    public List<Binding> getBindings() {
        return bindings;
    }

    @Override
    public String toString() {
        return "LothbrokBindings{" +
                "bindings=" + bindings +
                '}';
    }
}
