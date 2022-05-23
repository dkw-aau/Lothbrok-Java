package org.lothbrok.utils;

import org.lothbrok.stars.StarString;
import org.rdfhdt.hdt.triples.TripleString;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class FragmentGeneratorIterator implements Iterator<TripleString> {
    private Iterator<StarString> currIterator = null;
    private Queue<TripleString> currStar = new LinkedList<>();
    private TripleString curr = null;

    public FragmentGeneratorIterator(Iterator<StarString> it) {
        currIterator = it;
    }

    private void buffer() {
        if(curr != null) return;
        if(currStar.size() == 0) {
            if(currIterator != null) {
                if(!currIterator.hasNext()) {
                    currIterator = null;
                    return;
                }

                StarString st = currIterator.next();
                currStar.addAll(st.toTripleStrings());
            }
        }

        if(currStar.size() > 0) {
            curr = currStar.poll();
        }
    }

    @Override
    public boolean hasNext() {
        if(curr == null) buffer();
        return curr != null;
    }

    @Override
    public TripleString next() {
        TripleString ts = curr;
        curr = null;
        return ts;
    }
}
