package org.rdfhdt.hdt.triples.impl;

import org.lothbrok.stars.StarID;
import org.lothbrok.utils.Tuple;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.Triples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NestedStarIterator {
    private Triples triples;
    private StarID originalStar;
    private Map<String, Integer> currentBindings;
    protected List<TripleID> currentTriples;
    private StarID star;
    private TripleID selected = null;
    private NestedStarIterator iterator = null;
    private Tuple<String, String> vars = null;
    private IteratorTripleID it = null;
    private List<TripleID> next = null;


    public NestedStarIterator() {
    }

    public NestedStarIterator(Triples triples,
                              StarID originalStar,
                              Map<String, Integer> currentBindings,
                              List<TripleID> currentTriples) {
        this.triples = triples;
        this.originalStar = originalStar;
        this.currentBindings = currentBindings;
        this.currentTriples = currentTriples;
        star = new StarID(originalStar);
        selectNextTriple();
        iterator = new EmptyNestedIterator(currentTriples);
        it = triples.search(selected);
    }

    public NestedStarIterator(Triples triples,
                              StarID originalStar) {
        this(triples, originalStar, new HashMap<>(), new ArrayList<>());
    }

    public List<TripleID> getNext() {
        if(next == null) bufferNext();
        List<TripleID> ret = next;
        next = null;
        return ret;
    }

    public boolean hasNext() {
        if(next == null)
            bufferNext();
        return next != null;
    }

    private void bufferNext() {
        while(true) {
            if (iterator.hasNext()) {
                next = iterator.getNext();
                return;
            }

            if (!it.hasNext()) {
                next = null;
                return;
            }

            TripleID tid = it.next();

            Map<String, Integer> bindings = new HashMap<>(currentBindings);
            if(!currentBindings.containsKey(star.getSubjVar()))
                bindings.put(star.getSubjVar(), (int)tid.getSubject());
            if (!vars.x.equals(""))
                bindings.put(vars.x, (int) tid.getPredicate());
            if (!vars.y.equals(""))
                bindings.put(vars.y, (int) tid.getObject());

            List<TripleID> triples = new ArrayList<>(currentTriples);
            triples.add(tid);
            if(star.size() == 0) {
                next = triples;
                return;
            }
            iterator = new NestedStarIterator(this.triples, star, bindings, triples);
        }
    }

    private void selectNextTriple() {
        if (star.size() == 0) return;
        if (star.size() == 1) {
            selected = star.getTriple(0);
            vars = star.getVar(0);
            star.remove(0);
            setNextSelected();
            return;
        }
        int currIndex = 0;
        long currBest = Long.MAX_VALUE;

        for (int i = 0; i < star.size(); i++) {
            TripleID triple = replace(star.getTriple(i), star.getVar(i));
            IteratorTripleID tmpIt = triples.search(triple);

            long cnt = tmpIt.estimatedNumResults();
            if (cnt < currBest) {
                currIndex = i;
                currBest = cnt;
            }
        }

        selected = star.getTriple(currIndex);
        vars = star.getVar(currIndex);
        star.remove(currIndex);
        setNextSelected();
    }

    private void setNextSelected() {
        if (selected.getSubject() == 0 && currentBindings.containsKey(star.getSubjVar()))
            selected.setSubject(currentBindings.get(star.getSubjVar()));
        if (selected.getPredicate() == 0 && currentBindings.containsKey(vars.x))
            selected.setPredicate(currentBindings.get(vars.x));
        if (selected.getObject() == 0 && currentBindings.containsKey(vars.y))
            selected.setObject(currentBindings.get(vars.y));
    }

    private TripleID replace(TripleID triple, Tuple<String, String> vars) {
        if (triple.getSubject() == 0 && currentBindings.containsKey(star.getSubjVar()))
            triple.setSubject(currentBindings.get(star.getSubjVar()));
        if (triple.getPredicate() == 0 && currentBindings.containsKey(vars.x))
            triple.setPredicate(currentBindings.get(vars.x));
        if (triple.getObject() == 0 && currentBindings.containsKey(vars.y))
            triple.setObject(currentBindings.get(vars.y));
        return triple;
    }
}
