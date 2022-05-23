package org.lothbrok.sparql.iter;

import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.n3.turtle.parser.ParseException;
import org.apache.jena.n3.turtle.parser.TurtleParser;
import org.apache.jena.reasoner.rulesys.Util;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.engine.binding.BindingMap;
import org.apache.jena.util.iterator.NiceIterator;
import org.lothbrok.sparql.LothbrokBindings;
import org.lothbrok.sparql.LothbrokJenaConstants;
import org.lothbrok.sparql.LothbrokTurtleEventHandler;
import org.lothbrok.stars.StarString;
import org.lothbrok.utils.Tuple;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class RemoteLothbrokSingleIterator extends NiceIterator<Pair<StarString, Binding>> {
    private String currUrl;
    private final StarString star;
    private final LothbrokBindings bindings;
    private Queue<Pair<StarString, Binding>> results = new LinkedList<>();
    private Pair<StarString, Binding> next = null;

    public RemoteLothbrokSingleIterator(String startUrl, StarString star, LothbrokBindings bindings) {
        //System.out.println("Remote: " + triple.toString() + " \nStartURL: " + startUrl);
        this.currUrl = startUrl;
        this.star = star;
        this.bindings = bindings;
    }

    private void bufferNext() {
        if (results.size() == 0) {
            while(currUrl != null) {
                parseNext();
                if(results.size() > 0) {
                    next = results.poll();
                    return;
                }
            }
            return;
        }

        next = results.poll();
    }

    @Override
    public boolean hasNext() {
        if (next == null) bufferNext();
        return next != null;
    }

    @Override
    public Pair<StarString, Binding> next() {
        Pair<StarString, Binding> r = next;
        next = null;
        return r;
    }

    private void parseNext() {
        if (currUrl == null) return;
        Content content = null;
        try {
            LothbrokJenaConstants.NEM++;
            content = Request.Get(currUrl).addHeader("accept", "text/turtle").execute().returnContent();
            LothbrokJenaConstants.NTB += content.asBytes().length;
        } catch (IOException e) {
            currUrl = null;
            return;
        }
        //System.out.println(content.asString());
        TurtleParser parser = new TurtleParser(content.asStream());
        LothbrokTurtleEventHandler handler = new LothbrokTurtleEventHandler(currUrl);
        parser.setEventHandler(handler);
        try {
            parser.parse();
        } catch (ParseException e) {
            e.printStackTrace();
            currUrl = null;
            return;
        }

        List<StarString> lst = handler.getStars();
        //System.out.println("Results: " + lst.toString());
        if (handler.hasNextPage()) currUrl = handler.getNextPageUrl();
        else currUrl = null;
        results = extendBindings(bindings, star, lst);
    }

    private Queue<Pair<StarString, Binding>> extendBindings(LothbrokBindings bindings, StarString star, List<StarString> stars) {
        Queue<Pair<StarString, Binding>> extendedBindings = new LinkedList<>();
        if (bindings.size() == 0 || bindings.get(0).isEmpty()) {
            for (StarString str : stars) {
                BindingMap b;
                if(bindings.size() == 0)
                    b = new BindingHashMap();
                else
                    b = (BindingMap) bindings.get(0);

                if (star.getSubject().charAt(0) == '?')
                    b.add(Var.alloc(star.getSubject().toString().replace("?", "")), getNode(str.getSubject().toString()));

                for (int i = 0; i < star.size(); i++) {
                    Tuple<CharSequence, CharSequence> tpl1 = star.getTripleAt(i);
                    Tuple<CharSequence, CharSequence> tpl2 = getFirstMatching(str, tpl1, i);

                    if (tpl1.x.charAt(0) == '?')
                        b.add(Var.alloc(tpl1.x.toString().replace("?", "")), getNode(tpl2.x.toString()));
                    if (tpl1.y.charAt(0) == '?')
                        b.add(Var.alloc(tpl1.y.toString().replace("?", "")), getNode(tpl2.y.toString()));
                }
                extendedBindings.add(new Pair<>(str, b));
            }
        } else {
            int size = bindings.size();
            for (int i = 0; i < size; i++) {
                Binding currentBinding = bindings.get(i);
                for (StarString str : stars) {
                    if (matchesWithBinding(star, str, currentBinding)) {
                        BindingMap b = createCopy((BindingMap) currentBinding);

                        if (star.getSubject().charAt(0) == '?')
                            b.add(Var.alloc(star.getSubject().toString().replace("?", "")), getNode(str.getSubject().toString()));

                        for (int j = 0; j < star.size(); j++) {
                            Tuple<CharSequence, CharSequence> tpl1 = star.getTripleAt(j);
                            Tuple<CharSequence, CharSequence> tpl2 = getFirstMatching(str, tpl1, j);

                            if (tpl1.x.charAt(0) == '?')
                                b.add(Var.alloc(tpl1.x.toString().replace("?", "")), getNode(tpl2.x.toString()));
                            if (tpl1.y.charAt(0) == '?')
                                b.add(Var.alloc(tpl1.y.toString().replace("?", "")), getNode(tpl2.y.toString()));
                        }

                        extendedBindings.add(new Pair<>(star, b));
                    }
                }
            }

        }
        return extendedBindings;
    }

    private BindingMap createCopy(BindingMap b) {
        BindingMap binding = BindingFactory.create();

        Iterator<Var> it = b.vars();
        while(it.hasNext()) {
            Var v = it.next();
            binding.add(v, b.get(v));
        }

        return binding;
    }

    private Tuple<CharSequence, CharSequence> getFirstMatching(StarString nxt, Tuple<CharSequence, CharSequence> tpl, int index) {
        //if(tpl.x.charAt(0) == '?' && tpl.y.charAt(0) == '?') return nxt.getTripleAt(index);
        for(Tuple<CharSequence, CharSequence> tpl1 : nxt.getTriples()) {
            if(tpl.x.charAt(0) == '?' && tpl.y.equals(tpl1.y)) return tpl1;
            if(tpl.y.charAt(0) == '?' && tpl.x.equals(tpl1.x)) return tpl1;
        }
        return nxt.getTripleAt(index);
    }

    private boolean matchesWithBinding(StarString str, StarString star, Binding binding) {
        if (str.getSubject().charAt(0) == '?' && binding.contains(Var.alloc(str.getSubject().toString().replace("?", "")))) {
            if (!binding.get(Var.alloc(str.getSubject().toString().replace("?", ""))).getURI().equals(star.getSubject().toString())) {
                return false;
            }

        }

        for(Tuple<CharSequence, CharSequence> tpl1 : str.getTriples()) {
            Tuple<CharSequence, CharSequence> tpl2 = star.getFirstTripleWithPredicate(tpl1.x.toString());
            if (tpl1.x.charAt(0) == '?' && binding.contains(Var.alloc(tpl1.x.toString().replace("?", "")))) {
                if (!binding.get(Var.alloc(tpl1.x.toString().replace("?", ""))).getURI().equals(tpl2.x.toString())) {
                    return false;
                }
            }

            if (tpl1.y.charAt(0) == '?' && binding.contains(Var.alloc(tpl1.y.toString().replace("?", "")))) {
                Node node = binding.get(Var.alloc(tpl1.y.toString().replace("?", "")));
                String str1;
                if(node.isURI()) str1 = node.getURI();
                else str1 = node.getLiteralLexicalForm();
                if (!str1.equals(tpl2.y.toString())) {
                    return false;
                }
            }
        }
        return true;

    }

    private final static String regex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
    private Node getNode(String element) {
        if (element.length() == 0) return NodeFactory.createBlankNode();
        char firstChar = element.charAt(0);
        if (firstChar == '_') {
            return NodeFactory.createBlankNode(element);
        } else if (element.matches(regex)) {
            return NodeFactory.createURI(element);
        } else {
            String noq = element.replace("\"", "");
            if (noq.matches("-?\\d+")) {
                return Util.makeIntNode(Integer.parseInt(noq));
            } else if (noq.matches("([0-9]+)\\.([0-9]+)")) {
                return Util.makeDoubleNode(Double.parseDouble(noq));
            }
            return NodeFactory.createLiteral(element.replace("\"", ""));
        }
    }
}
