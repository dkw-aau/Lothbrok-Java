package org.lothbrok.sparql.iter;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Node_Variable;
import org.apache.jena.reasoner.rulesys.Util;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.binding.BindingMap;
import org.apache.jena.util.iterator.NiceIterator;
import org.lothbrok.sparql.LothbrokBindings;
import org.lothbrok.stars.StarString;
import org.lothbrok.utils.Tuple;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorStarString;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LocalLothbrokSingleIterator extends NiceIterator<Pair<StarString, Binding>> {
    private StarString star = null;
    private final HDT datasource;
    private final IteratorStar it;
    private IteratorStarString iterator = null;
    private Pair<StarString, Binding> next = null;

    public LocalLothbrokSingleIterator(StarString star, LothbrokBindings bindings, HDT datasource) {
        this.datasource = datasource;
        this.it = new IteratorStar(star, bindings);
        if (it.hasNext()) {
            this.star = it.next();
            iterator = datasource.searchStar(this.star);
        }
    }

    String regex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
    private void bufferNext() {
        next = null;
        if (iterator == null) return;
        if (iterator.hasNext()) {
            StarString nxt = iterator.next();
            Binding b = it.getCurrBinding();
            if (b == null) b = BindingFactory.create();

            if(star.getSubject().charAt(0) == '?')
                ((BindingMap) b).add(Var.alloc(star.getSubject().toString().replace("?", "")), getNode(nxt.getSubject().toString()));

            for(int i = 0; i < star.size(); i++) {
                Tuple<CharSequence, CharSequence> tpl1 = star.getTripleAt(i);
                Tuple<CharSequence, CharSequence> tpl2 = getFirstMatching(nxt, tpl1, i);

                if(tpl1.x.charAt(0) == '?')
                    ((BindingMap) b).add(Var.alloc(tpl1.x.toString().replace("?", "")), getNode(tpl2.x.toString()));
                if(tpl1.y.charAt(0) == '?')
                    ((BindingMap) b).add(Var.alloc(tpl1.y.toString().replace("?", "")), getNode(tpl2.y.toString()));
            }

            next = new Pair<>(nxt, b);
            return;
        }

        if (it.hasNext()) {
            this.star = it.next();
            iterator = datasource.searchStar(this.star);
            bufferNext();
        }
    }

    private Tuple<CharSequence, CharSequence> getFirstMatching(StarString nxt, Tuple<CharSequence, CharSequence> tpl, int index) {
        //if(tpl.x.charAt(0) == '?' && tpl.y.charAt(0) == '?') return nxt.getTripleAt(index);

        for(Tuple<CharSequence, CharSequence> tpl1 : nxt.getTriples()) {
            if(tpl.x.charAt(0) == '?' && tpl.y.toString().equals(tpl1.y.toString())) return tpl1;
            if(tpl.y.charAt(0) == '?' && tpl.x.toString().equals(tpl1.x.toString())) return tpl1;
        }
        return nxt.getTripleAt(index);
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

    private Node getNode(String element) {
        if (element.length() == 0) return NodeFactory.createBlankNode();
        char firstChar = element.charAt(0);
        if (firstChar == '_') {
            return NodeFactory.createBlankNode(element);
        } else if (firstChar == '"') {
            String noq = element.replace("\"", "");
            if (noq.matches("-?\\d+")) {
                return Util.makeIntNode(Integer.parseInt(noq));
            } else if (noq.matches("([0-9]+)\\.([0-9]+)")) {
                return Util.makeDoubleNode(Double.parseDouble(noq));
            }
            return NodeFactory.createLiteral(element.replace("\"", ""));
        } else {
            return NodeFactory.createURI(element);
        }
    }

    private static class IteratorStar implements Iterator<StarString> {
        private final StarString star;
        private final LothbrokBindings bindings;
        private int currId = 0;
        private StarString next = null;
        private Binding currBinding = null;

        public IteratorStar(StarString star, LothbrokBindings bindings) {
            this.star = star;
            this.bindings = bindings;
        }

        private void bufferNext() {
            next = null;
            if (currId == 0 && (bindings.size() == 0 || bindings.get(0).isEmpty())) {
                next = star;
                currId++;
                return;
            }

            if (currId >= bindings.size()) return;
            Binding b = bindings.get(currId);
            currBinding = b;
            currId++;

            String subj;
            if (star.getSubject().charAt(0) == '?' && b.contains(Var.alloc(star.getSubject().toString().replace("?", "")))) {
                Node n = b.get(Var.alloc(star.getSubject().toString().replace("?", "")));
                subj = n.isURI() ? n.getURI() : n.getLiteral().toString();
            } else {
                subj = star.getSubject().toString();
            }

            List<Tuple<CharSequence, CharSequence>> tpl = new ArrayList<>();
            for (Tuple<CharSequence, CharSequence> t : star.getTriples()) {
                String pred;
                if (t.x.charAt(0) == '?' && b.contains(Var.alloc(t.x.toString().replace("?", "")))) {
                    Node n = b.get(Var.alloc(t.x.toString().replace("?", "")));
                    pred = n.isURI() ? n.getURI() : n.getLiteral().toString();
                } else {
                    pred = t.x.toString();
                }

                String obj;
                if (t.y.charAt(0) == '?' && b.contains(Var.alloc(t.y.toString().replace("?", "")))) {
                    Node n = b.get(Var.alloc(t.y.toString().replace("?", "")));
                    obj = n.isURI() ? n.getURI() : n.getLiteral().toString();
                } else {
                    obj = t.y.toString();
                }

                tpl.add(new Tuple<>(pred, obj));
            }

            next = new StarString(subj, tpl);
        }

        @Override
        public boolean hasNext() {
            if (next == null) bufferNext();
            return next != null;
        }

        @Override
        public StarString next() {
            StarString t = next;
            next = null;
            return t;
        }

        public Binding getCurrBinding() {
            return currBinding;
        }
    }
}
