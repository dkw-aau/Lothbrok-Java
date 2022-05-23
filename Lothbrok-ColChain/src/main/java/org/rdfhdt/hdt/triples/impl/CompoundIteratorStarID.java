package org.rdfhdt.hdt.triples.impl;

import org.lothbrok.stars.StarID;
import org.lothbrok.stars.StarString;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorStarID;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.Triples;

import java.util.*;

public class CompoundIteratorStarID implements IteratorStarID {
    private StarID next = null;
    private final Triples triples;
    private final StarID star;
    private Set<StarID> cache = new HashSet<>();

    private final NestedStarIterator iterator;

    public CompoundIteratorStarID(HDT hdt, StarString star) {
        this(star.toStarID(hdt.getDictionary()), hdt.getTriples());
    }

    public CompoundIteratorStarID(StarID star, Triples triples) {
        this.triples = triples;
        this.star = star;
        this.iterator = new NestedStarIterator(this.triples, this.star);
    }

    @Override
    public long estimatedNumResults() {
        return 0;
    }

    @Override
    public ResultEstimationType numResultEstimation() {
        return ResultEstimationType.APPROXIMATE;
    }

    @Override
    public boolean hasNext() {
        while(next == null) {
            buffer();
            if(next == null) break;
            if(cache.contains(next)) {
                next = null;
                continue;
            }
            cache.add(next);
        }

        return next != null;
    }

    @Override
    public StarID next() {
        StarID ret = next;
        next = null;
        return ret;
    }

    private void buffer() {
        if(!iterator.hasNext()) {
            next = null;
            return;
        }

        List<TripleID> triples = iterator.getNext();
        next = new StarID(triples);
    }
}
