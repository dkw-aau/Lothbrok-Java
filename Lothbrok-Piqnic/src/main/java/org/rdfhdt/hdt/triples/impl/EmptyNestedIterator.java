package org.rdfhdt.hdt.triples.impl;

import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.Triples;

import java.util.List;
import java.util.Map;

public class EmptyNestedIterator extends NestedStarIterator {
    public EmptyNestedIterator(List<TripleID> currentTriples) {
        this.currentTriples = currentTriples;
    }

    @Override
    public List<TripleID> getNext() {
        return currentTriples;
    }

    @Override
    public boolean hasNext() {
        return false;
    }
}
