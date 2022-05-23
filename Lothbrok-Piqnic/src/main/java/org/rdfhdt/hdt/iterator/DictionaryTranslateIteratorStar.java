package org.rdfhdt.hdt.iterator;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.lothbrok.stars.StarString;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.*;
import org.rdfhdt.hdt.triples.impl.CompoundIteratorStarID;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DictionaryTranslateIteratorStar implements IteratorStarString {
    /**
     * The iterator of TripleID
     */
    private IteratorStarID currentIterator;
    /**
     * The dictionary
     */
    final StarStringIterator iterator;
    final HDT hdt;
    private StarString curr;
    private StarString next = null;
    private long numResultEstimation = 0;

    public DictionaryTranslateIteratorStar(StarString star, HDT hdt) {
        this(star, new ArrayList<>(), hdt);
    }

    public DictionaryTranslateIteratorStar(StarString star, List<Binding> bindings, HDT hdt) {
        this.iterator = new StarStringIterator(bindings, star);
        this.hdt = hdt;
        this.curr = iterator.next();
        this.currentIterator = new CompoundIteratorStarID(this.hdt, this.curr);
        estimateNumResultsNoCs(star, bindings);
    }

    /*public static long estimateCardinality(StarString star, List<Binding> bindings, List<ICharacteristicSet> characteristicSets) {
        long numResultEstimation = 0;
        final StarStringIterator it = new StarStringIterator(bindings, star);
        List<ICharacteristicSet> css = new ArrayList<>();
        for (ICharacteristicSet cs : characteristicSets) {
            if (cs.matches(star)) css.add(cs);
        }

        while (it.hasNext()) {
            StarString st = it.next();
            double size = 0;
            for (ICharacteristicSet cs : css) {
                size += cs.count(st);
            }
            numResultEstimation += (long) size;
        }

        return numResultEstimation;
    }

    private void estimateNumResults(StarString star, List<Binding> bindings, List<ICharacteristicSet> characteristicSets) {
        if(characteristicSets == null || characteristicSets.size() == 0) {
            estimateNumResultsNoCs(star, bindings);
            return;
        }
        final StarStringIterator it = new StarStringIterator(bindings, star);
        List<ICharacteristicSet> css = new ArrayList<>();
        for (ICharacteristicSet cs : characteristicSets) {
            if (cs.matches(star)) css.add(cs);
        }

        while (it.hasNext()) {
            StarString st = it.next();
            double size = 0;
            for (ICharacteristicSet cs : css) {
                size += cs.count(st);
            }
            numResultEstimation += (long) size;
        }
        if(numResultEstimation == 0)
            estimateNumResultsNoCs(star, bindings);
    }*/

    private void estimateNumResultsNoCs(StarString star, List<Binding> bindings) {
        StarStringIterator it = new StarStringIterator(bindings, star);
        while(it.hasNext()) {
            numResultEstimation += estimateNumResultsStar(it.next());
        }
    }

    private long estimateNumResultsStar(StarString star) {
        long size = hdt.getDictionary().getSubjects().getNumberOfElements();
        double nSize = size;
        int s = star.size();
        for(int i = 0; i < s; i++) {
            TripleString t = star.getTripleString(i);
            if(!t.getSubject().equals("") || !t.getObject().equals("")) {
                try {
                    IteratorTripleString it = hdt.search(t.getSubject(), t.getPredicate(), t.getObject());
                    double mult = (double)it.estimatedNumResults() / (double)size;
                    nSize = nSize * mult;
                } catch (NotFoundException e) { continue; }
            }
        }

        return (long)nSize;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        while (next == null) {
            buffer();
            if (next == null) break;
        }

        return next != null;
    }

    private void buffer() {
        if (currentIterator.hasNext()) {
            next = currentIterator.next().toStarString(hdt.getDictionary());
            return;
        }

        if (iterator.hasNext()) {
            this.curr = iterator.next();
            currentIterator = new CompoundIteratorStarID(this.hdt, curr);
            buffer();
            return;
        }

        next = null;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#next()
     */
    @Override
    public StarString next() {
        if(next == null) buffer();
        StarString ret = next;
        next = null;
        return ret;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        iterator.remove();
    }

    @Override
    public long estimatedNumResults() {
        return numResultEstimation;
    }

    @Override
    public ResultEstimationType numResultEstimation() {
        return ResultEstimationType.APPROXIMATE;
    }

    private static class StarStringIterator implements Iterator<StarString> {
        private final List<Binding> bindings;
        private final StarString star;
        private int current = 0;
        private StarString next = null;

        StarStringIterator(List<Binding> bindings, StarString star) {
            this.bindings = bindings;
            this.star = star;
        }

        private void bufferNext() {
            if ((bindings == null || bindings.size() == 0) && current == 0) {
                next = star;
                current++;
                return;
            }
            if (bindings == null) {
                next = null;
                return;
            }
            if (current >= bindings.size()) {
                next = null;
                return;
            }
            Binding binding = bindings.get(current);
            current++;
            StarString s = new StarString(star.getSubject(), star.getTriples());

            Iterator<Var> vars = binding.vars();
            while (vars.hasNext()) {
                Var var = vars.next();
                Node node = binding.get(var);

                String val = "";
                if (node.isLiteral())
                    val = node.getLiteral().toString();
                else if (node.isURI())
                    val = node.getURI();

                s.updateField(var.getVarName(), val);
            }

            next = s;
        }

        public void reset() {
            current = 0;
        }

        @Override
        public boolean hasNext() {
            if (next == null)
                bufferNext();
            return next != null;
        }

        @Override
        public StarString next() {
            if(next == null) bufferNext();
            StarString n = next;
            next = null;
            return n;
        }
    }
}
