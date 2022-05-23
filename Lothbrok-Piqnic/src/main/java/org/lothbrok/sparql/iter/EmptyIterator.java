package org.lothbrok.sparql.iter;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.util.iterator.NiceIterator;
import org.lothbrok.stars.StarString;

import java.util.NoSuchElementException;

public class EmptyIterator extends NiceIterator<Pair<StarString, Binding>> {
    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Pair<StarString, Binding> next() {
        throw new NoSuchElementException();
    }
}
