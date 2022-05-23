package org.lothbrok.sparql.iter;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.lothbrok.sparql.LothbrokBindings;
import org.lothbrok.sparql.graph.LothbrokGraph;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.IQueryStrategy;
import org.lothbrok.strategy.impl.UnionQueryStrategy;

import java.util.LinkedList;
import java.util.Queue;

public class LocalLothbrokUnionIterator extends NiceIterator<Pair<StarString, Binding>> {
    private final Queue<ExtendedIterator<Pair<StarString, Binding>>> iterators;
    private ExtendedIterator<Pair<StarString, Binding>> currIt;
    private Pair<StarString, Binding> next = null;

    public LocalLothbrokUnionIterator(Queue<ExtendedIterator<Pair<StarString, Binding>>> iterators) {
        this.iterators = iterators;
        this.currIt = iterators.poll();
    }

    public LocalLothbrokUnionIterator(UnionQueryStrategy strategy, LothbrokGraph graph, LothbrokBindings bindings) {
        iterators = new LinkedList<>();
        for(IQueryStrategy strategy1 : strategy.getStrategies()) {
            iterators.add(graph.graphBaseFind(strategy1, bindings));
        }
        currIt = iterators.poll();
    }

    private void bufferNext() {
        if(next != null) return;
        if(currIt == null) return;
        if(currIt.hasNext()) {
            next = currIt.next();
            return;
        }

        if(!iterators.isEmpty()) {
            currIt = iterators.poll();
            bufferNext();
        }
    }

    @Override
    public boolean hasNext() {
        if (next == null) bufferNext();
        return next != null;
    }

    @Override
    public Pair<StarString, Binding> next() {
        Pair<StarString, Binding> p = next;
        next = null;
        return p;
    }
}
