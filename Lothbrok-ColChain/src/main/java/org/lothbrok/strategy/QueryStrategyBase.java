package org.lothbrok.strategy;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.lothbrok.sparql.LothbrokBindings;
import org.lothbrok.sparql.graph.LothbrokGraph;
import org.lothbrok.stars.StarString;

public abstract class QueryStrategyBase implements IQueryStrategy {
    private final Type type;
    private ExtendedIterator<Pair<StarString, Binding>> currIt = null;
    private Binding next = null;

    public QueryStrategyBase(Type type) {
        this.type = type;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public boolean isEmpty() {
        return type == Type.EMPTY;
    }

    @Override
    public boolean hasNextBinding(LothbrokGraph graph) {
        if(next == null) bufferNext(graph, new LothbrokBindings());
        return next != null;
    }

    @Override
    public boolean hasNextBinding(LothbrokGraph graph, LothbrokBindings bindings) {
        if(next == null) bufferNext(graph, bindings);
        return next != null;
    }

    private void bufferNext(LothbrokGraph graph, LothbrokBindings bindings) {
        if(currIt == null) currIt = graph.graphBaseFind(this, bindings);
        if(currIt == null) return;

        if(currIt.hasNext()) {
            Pair<StarString, Binding> pair = currIt.next();
            this.next = pair.getRight();
            return;
        }

        next = null;
    }

    @Override
    public Binding moveToNextBinding(LothbrokGraph graph) {
        if (next == null) bufferNext(graph, new LothbrokBindings());
        Binding b = next;
        next = null;
        return b;
    }

    @Override
    public Binding moveToNextBinding(LothbrokGraph graph, LothbrokBindings bindings) {
        if (next == null) bufferNext(graph, bindings);
        Binding b = next;
        next = null;
        return b;
    }

    public enum Type {
        JOIN, UNION, SINGLE, EMPTY
    }
}
