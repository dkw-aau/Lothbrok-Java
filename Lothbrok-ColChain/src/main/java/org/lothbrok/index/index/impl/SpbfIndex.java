package org.lothbrok.index.index.impl;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.n3.turtle.parser.ParseException;
import org.apache.jena.n3.turtle.parser.TurtleParser;
import org.apache.jena.reasoner.rulesys.Util;
import org.apache.jena.tdb.store.Hash;
import org.colchain.colchain.node.AbstractNode;
import org.lothbrok.characteristicset.ICharacteristicSet;
import org.lothbrok.compatibilitygraph.CompatibilityGraph;
import org.lothbrok.compatibilitygraph.CompatibleGraphs;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.index.index.IndexMapping;
import org.lothbrok.index.index.PartitionedIndexBase;
import org.lothbrok.index.ppbf.IBloomFilter;
import org.lothbrok.index.ppbf.IPartitionedBloomFilter;
import org.lothbrok.sparql.LothbrokBindings;
import org.lothbrok.sparql.LothbrokJenaConstants;
import org.lothbrok.sparql.LothbrokTurtleEventHandler;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.IQueryStrategy;
import org.lothbrok.strategy.QueryStrategyBase;
import org.lothbrok.strategy.dp.DpTable;
import org.lothbrok.strategy.dp.DpTableEntry;
import org.lothbrok.strategy.impl.QueryStrategyFactory;
import org.lothbrok.strategy.impl.UnionQueryStrategy;
import org.lothbrok.utils.Triple;
import org.lothbrok.utils.Tuple;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.triples.TripleString;

import java.io.IOException;
import java.util.*;

public class SpbfIndex extends PartitionedIndexBase {
    //private Map<Tuple<IGraph, IGraph>, IBloomFilter<String>> blooms = new HashMap<>();
    private Map<Tuple<Tuple<IGraph, IGraph>, String>, IBloomFilter<String>> blooms = new HashMap<>();
    private Map<IGraph, IPartitionedBloomFilter<String>> bs = new HashMap<>();
    private Set<IGraph> fragments = new HashSet<>();
    private Map<String, Set<IGraph>> fragmentMap = new HashMap<>();

    public SpbfIndex(Map<Tuple<Tuple<IGraph, IGraph>, String>, IBloomFilter<String>> blooms, Map<IGraph, IPartitionedBloomFilter<String>> bs, Set<IGraph> fragments) {
        this.blooms = blooms;
        this.bs = bs;
        this.fragments = fragments;

        for (IGraph fragment : fragments) {
            ICharacteristicSet cs = fragment.getCharacteristicSet();
            for (String pred : cs.getPredicates()) {
                if (fragmentMap.containsKey(pred)) fragmentMap.get(pred).add(fragment);
                else {
                    Set<IGraph> frags = new HashSet<>();
                    frags.add(fragment);
                    fragmentMap.put(pred, frags);
                }
            }
        }
    }

    public SpbfIndex() {
    }

    public void addFragment(IGraph graph, IPartitionedBloomFilter<String> filter) {
        fragments.add(graph);
        bs.put(graph, filter);

        ICharacteristicSet cs = graph.getCharacteristicSet();
        for (String pred : cs.getPredicates()) {
            if (fragmentMap.containsKey(pred)) fragmentMap.get(pred).add(graph);
            else {
                Set<IGraph> frags = new HashSet<>();
                frags.add(graph);
                fragmentMap.put(pred, frags);
            }
        }
    }

    /*public Map<Tuple<IGraph, IGraph>, IBloomFilter<String>> getBlooms() {
        return blooms;
    }

    public Map<IGraph, IPartitionedBloomFilter<String>> getBs() {
        return bs;
    }*/

    public Map<Tuple<Tuple<IGraph, IGraph>, String>, IBloomFilter<String>> getBlooms() {
        return blooms;
    }

    public Map<IGraph, IPartitionedBloomFilter<String>> getBs() {
        return bs;
    }

    @Override
    public boolean isBuilt() {
        return bs.size() > 0;
    }

    @Override
    public IndexMapping getMapping(List<Triple> query) {
        /*Map<Triple, Set<IGraph>> identifiable = new HashMap<>();
        for (Triple t : query) {
            identifiable.put(t, new HashSet<>());
        }

        String regex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        for (IGraph f : fragments) {
            for (Triple t : query) {
                //if (f.identify(t)) {
                    if (t.getSubject().matches(regex)) {
                        if (!bs.get(f).mightContainSubject(t.getSubject())) {
                            continue;
                        }
                    }
                    if (t.getPredicate().matches(regex)) {
                        if (!bs.get(f).mightContainPredicate(t.getPredicate())) {
                            continue;
                        }
                    }
                    if (t.getObject().matches(regex)) {
                        if (!bs.get(f).mightContainObject(t.getObject(), t.getPredicate())) {
                            continue;
                        }
                    }

                    identifiable.get(t).add(f);
                //}
            }
        }

        Map<Tuple<Triple, Triple>, String> bindings = new HashMap<>();
        for (Triple t1 : query) {
            for (Triple t2 : query) {
                if (t1.equals(t2) || bindings.containsKey(new Tuple<>(t1, t2)) || bindings.containsKey(new Tuple<>(t2, t1)))
                    continue;
                String b = bound(t1, t2);
                if (b != null && b.startsWith("?")) {
                    bindings.put(new Tuple<>(t1, t2), b);
                }
            }
        }

        boolean change = true;
        while (change) {
            change = false;

            for (Map.Entry<Tuple<Triple, Triple>, String> binding : bindings.entrySet()) {
                Triple t1 = binding.getKey().x;
                Triple t2 = binding.getKey().y;

                Set<IGraph> f1s = identifiable.get(t1);
                Set<IGraph> f2s = identifiable.get(t2);

                Set<IGraph> good1 = new HashSet<>(f1s.size());
                Set<IGraph> good2 = new HashSet<>(f2s.size());

                for (IGraph f1 : f1s) {
                    for (IGraph f2 : f2s) {
                        if (f1.equals(f2)) {
                            good1.add(f1);
                            good2.add(f2);
                            continue;
                        }
                        Tuple<IGraph, IGraph> tuple = new Tuple<>(f1, f2);
                        if (!blooms.containsKey(tuple)) tuple = new Tuple<>(f2, f1);
                        if (blooms.containsKey(tuple)) {
                            IBloomFilter<String> intersection = blooms.get(tuple);
                            if (!intersection.isEmpty()) {
                                good1.add(f1);
                                good2.add(f2);
                            }
                        } else {
                            IBloomFilter<String> b1 = bs.get(f1).getPartition(t1, binding.getValue());
                            IBloomFilter<String> b2 = bs.get(f2).getPartition(t2, binding.getValue());

                            String filename = f1.getId() + "-" + b1.getFileName().substring(b1.getFileName().lastIndexOf("/") + 1) +  "--"
                                    + f2.getId() + "-" + b2.getFileName().substring(b2.getFileName().lastIndexOf("/") + 1) + ".ppbf";

                            IBloomFilter<String> intersection = b1.intersect(b2, filename);
                            blooms.put(tuple, intersection);
                            if (!intersection.isEmpty()) {
                                good1.add(f1);
                                good2.add(f2);
                            }
                        }
                    }
                }

                if (good1.size() < f1s.size()) {
                    change = true;
                    identifiable.put(t1, good1);
                }
                if (good2.size() < f2s.size()) {
                    change = true;
                    identifiable.put(t2, good2);
                }
            }
        }

        return new IndexMapping(identifiable);*/
        throw new NotImplementedException("Function not usable for Partitioned Bloom Filter Index.");
    }

    @Override
    public long estimateCardinality(StarString star) {
        boolean subjBound = (!star.getSubject().equals("") && !star.getSubject().toString().startsWith("?"));
        int starSize = star.size();
        List<String> preds = star.getPredicates();
        long count = 0;

        Set<IGraph> fragments = getFragmentsByStar(star);
        for (IGraph fragment : fragments) {
            IPartitionedBloomFilter<String> b = bs.get(fragment);
            long distinct = b.getSubjectPartition().elementCount();
            double o = 1, m = 1;
            for (int i = 0; i < starSize; i++) {
                TripleString triple = star.getTriple(i);
                long cnt, objects;
                if (triple.getPredicate().charAt(0) == '?') {
                    cnt = b.getTotalCardinality();
                    objects = cnt;
                } else {
                    IBloomFilter<String> bf = b.getPredicatePartition(triple.getPredicate().toString());
                    if (bf == null) {
                        cnt = 0;
                        objects = 0;
                    } else {
                        cnt = bf.estimatedCardinality();
                        objects = bf.elementCount();
                    }
                }
                double multiplicity = (double) cnt / (double) distinct;
                if (!triple.getObject().equals("") && !triple.getObject().toString().startsWith("?")) {
                    o = Double.min(o, 1.0 / (double) objects);
                } else {
                    if (multiplicity == 0) multiplicity = 1;
                    m = m * multiplicity;
                }
            }

            long card = (long) (distinct * m * o);
            //if (card == 0) card = (long) (distinct * o);
            //if (card == 0) card = estimateCardReq(star, fragment);
            card = subjBound ? card / distinct : card;
            count += card;
            //count += estimateCardReq(star, fragment);
        }

        return count;
    }

    @Override
    public long estimateCardinality(StarString star, IGraph fragment) {
        boolean subjBound = (!star.getSubject().equals("") && !star.getSubject().toString().startsWith("?"));
        int starSize = star.size();
        List<String> preds = star.getPredicates();
        long count = 0;

        IPartitionedBloomFilter<String> b = bs.get(fragment);
        if (!b.hasPredicates(preds)) return 0;
        long distinct = b.getSubjectPartition().elementCount();
        double o = 1, m = 1;
        for (int i = 0; i < starSize; i++) {
            TripleString triple = star.getTriple(i);
            IBloomFilter<String> bf = b.getPredicatePartition(triple.getPredicate().toString());

            long cnt = bf.estimatedCardinality();
            long objects = bf.elementCount();
            double multiplicity = (double) cnt / (double) distinct;
            if (!triple.getObject().equals("") && !triple.getObject().toString().startsWith("?")) {
                o = Double.min(o, 1.0 / (double) objects);
            } else {
                if (multiplicity == 0) multiplicity = 1;
                m = m * multiplicity;
            }
        }

        long card = (long) (distinct * m * o);
        //if (card == 0) card = (long) (distinct * o);
        //if (card == 0) card = estimateCardReq(star, fragment);
        card = subjBound ? card / distinct : card;
        count += card;

        return count;
        //return estimateCardReq(star, fragment);
    }

    @Override
    public long estimateJoinCardinality(StarString star, IGraph fragment, IQueryStrategy left) {
        if(LothbrokJenaConstants.DISTINCT)
            return estimateJoinCardinalityDistinct(star, fragment, left);

        long card = left.estimateCardinality(this);
        Set<StarString> stars = left.getJoiningStars(star);
        for(StarString star1 : stars) {
            List<String> vars = star1.getVariables();
            vars.retainAll(star.getVariables());

            Set<IGraph> fs = left.getTopFragments();
            IPartitionedBloomFilter<String> pb1 = bs.get(fragment);

            double minSel = 1.0;

            for (String var : vars) {
                long s1 = 0;
                for (IGraph fragment1 : fs) {
                    IPartitionedBloomFilter<String> pb2 = bs.get(fragment1);
                    Tuple<IGraph, IGraph> tpl1 = new Tuple<>(fragment, fragment1);
                    Tuple<IGraph, IGraph> tpl2 = new Tuple<>(fragment1, fragment);
                    IBloomFilter<String> intersection;
                    if (blooms.containsKey(new Tuple<>(tpl1, var))) {
                        intersection = blooms.get(new Tuple<>(tpl1, var));
                    } else if (blooms.containsKey(new Tuple<>(tpl2, var))) {
                        intersection = blooms.get(new Tuple<>(tpl2, var));
                    } else {
                        //System.out.println("            Finding intersection");
                        IBloomFilter<String> b1 = bs.get(fragment).getPartition(star, var);
                        IBloomFilter<String> b2 = bs.get(fragment1).getPartition(star1, var);

                        String filename;
                        try {
                            filename = fragment.getId() + "-" + b1.getFileName().substring(b1.getFileName().lastIndexOf("/") + 1) + "--"
                                    + fragment1.getId() + "-" + b2.getFileName().substring(b2.getFileName().lastIndexOf("/") + 1) + ".ppbf";
                        } catch (NullPointerException e) {
                            continue;
                        }

                        intersection = b1.intersect(b2, filename);
                        blooms.put(new Tuple<>(tpl1, var), intersection);
                    }

                    s1 += intersection.estimatedCardinality();
                }

                long s2 = 0;

                for (IGraph fragment1 : fs) {
                    IPartitionedBloomFilter<String> pb2 = bs.get(fragment1);
                    IBloomFilter<String> b1 = pb2.getPartition(star1, var);
                    if(b1 == null) continue;
                    s2 += b1.elementCount();
                }

                double div = (double) s1 / (double) s2;
                if(div < minSel) minSel = div;
            }

            card = (long) (card * minSel);

            for (TripleString tpl : star.toTripleStrings()) {
                if (vars.contains(tpl.getObject().toString())) continue;
                long c1 = pb1.getPredicatePartition(tpl.getPredicate().toString()).elementCount();
                long c2 = pb1.getSubjectPartition().elementCount();

                double div = (double) c1 / (double) c2;
                card = (long) (card * div);
            }
        }

        return card;

        //List<String> vars = left.getVars();
        //long card = left.estimateCardinality(this);
        //return estimateJoinCardinality(star, vars, card, fragment);
    }

    private long estimateJoinCardinalityDistinct(StarString star, IGraph fragment, IQueryStrategy left) {
        long card = left.estimateCardinality(this);

        Set<StarString> stars = left.getJoiningStars(star);
        for(StarString star1 : stars) {
            List<String> vars = star1.getVariables();
            vars.retainAll(star.getVariables());

            double minSel = 1.0;
            Set<IGraph> fs = left.getTopFragments();
            for (String var : vars) {
                long s1 = 0;
                for (IGraph fragment1 : fs) {
                    Tuple<IGraph, IGraph> tpl1 = new Tuple<>(fragment, fragment1);
                    Tuple<IGraph, IGraph> tpl2 = new Tuple<>(fragment1, fragment);
                    IBloomFilter<String> intersection;
                    if (blooms.containsKey(new Tuple<>(tpl1, var))) {
                        intersection = blooms.get(new Tuple<>(tpl1, var));
                    } else if (blooms.containsKey(new Tuple<>(tpl2, var))) {
                        intersection = blooms.get(new Tuple<>(tpl2, var));
                    } else {
                        //System.out.println("            Finding intersection");
                        IBloomFilter<String> b1 = bs.get(fragment).getPartition(star, var);
                        IBloomFilter<String> b2 = bs.get(fragment1).getPartition(star1, var);

                        String filename;
                        try {
                            filename = fragment.getId() + "-" + b1.getFileName().substring(b1.getFileName().lastIndexOf("/") + 1) + "--"
                                    + fragment1.getId() + "-" + b2.getFileName().substring(b2.getFileName().lastIndexOf("/") + 1) + ".ppbf";
                        } catch (NullPointerException e) {
                            continue;
                        }

                        intersection = b1.intersect(b2, filename);
                        blooms.put(new Tuple<>(tpl1, var), intersection);
                    }

                    s1 += intersection.estimatedCardinality();
                }

                long s2 = 0;

                for (IGraph fragment1 : fragments) {
                    s2 += bs.get(fragment1).getSubjectPartition().elementCount();
                }

                double div = (double) s1 / (double) s2;
                if(div < minSel) minSel = div;
            }

            card = (long) (card * minSel);
        }

        return card;
        //return estimateJoinCardinality(star, fragment, left);
    }

    //    @Override
//    public long estimateJoinCardinality(StarString star1, StarString star2) {
//        long card1 = estimateCardinality(star1);
//        long card2 = estimateCardinality(star2);
//        if (card1 <= card2)
//            return estimateJoinCardinality(star2, star1.getVariables(), card1);
//        return estimateJoinCardinality(star1, star2.getVariables(), card2);
//    }

//    @Override
    private long estimateJoinCardinality(StarString star, List<String> vars, long boundCount) {
        if (boundCount == 0) return 0;
        boolean subjBound = (!star.getSubject().equals("") && !star.getSubject().toString().startsWith("?"));
        boolean subjBoundVar = !subjBound && vars.contains(star.getSubject().toString());

        int starSize = star.size();
        long count = 0;

        //System.out.println("Star card: " + star);
        //System.out.println(vars.toString());

        Set<IGraph> fragments = getFragmentsByStar(star);
        for (IGraph fragment : fragments) {
            IPartitionedBloomFilter<String> b = bs.get(fragment);
            long distinct = b.getSubjectPartition().elementCount();
            double o = 1, m = 1;
            for (int i = 0; i < starSize; i++) {
                TripleString triple = star.getTriple(i);
                long cnt, objects;
                if (triple.getPredicate().charAt(0) == '?') {
                    cnt = b.getTotalCardinality();
                    objects = cnt;
                } else {
                    IBloomFilter<String> bf = b.getPredicatePartition(triple.getPredicate().toString());
                    cnt = bf.estimatedCardinality();
                    objects = bf.elementCount();
                }
                double multiplicity = (double) cnt / (double) distinct;
                if (!triple.getObject().equals("") && !triple.getObject().toString().startsWith("?")) {
                    o = Double.min(o, 1.0 / (double) objects);
                } else if (vars.contains(triple.getObject().toString())) {
                    //System.out.println("Bound var: " + triple.getObject() + " " + boundCount);
                    o = Double.min(o, 1 - (1.0 / (double) boundCount));
                } else {
                    if (multiplicity == 0) multiplicity = 1;
                    m = m * multiplicity;
                }
            }

            long card = (long) (distinct * m * o);
            if (card == 0) card = (long) (distinct * o);
            //if (card == 0) card = estimateCardReq(star, fragment);

            if (subjBound) {
                card = (long) ((double) card / (double) distinct);
            } else if (subjBoundVar) {
                card = (long) (card * ((double) distinct / (double) boundCount));
            }

            count += card;
        }

        return count;
    }

//    @Override
    private long estimateJoinCardinality(StarString star, List<String> vars, long boundCount, IGraph fragment) {
        if (boundCount == 0) return 0;
        boolean subjBound = (!star.getSubject().equals("") && !star.getSubject().toString().startsWith("?"));
        boolean subjBoundVar = !subjBound && vars.contains(star.getSubject().toString());

        int starSize = star.size();
        IPartitionedBloomFilter<String> b = bs.get(fragment);
        long distinct = b.getSubjectPartition().elementCount();
        double o = 1, m = 1;
        for (int i = 0; i < starSize; i++) {
            TripleString triple = star.getTriple(i);
            long cnt, objects;
            if (triple.getPredicate().charAt(0) == '?') {
                cnt = b.getTotalCardinality();
                objects = cnt;
            } else {
                IBloomFilter<String> bf = b.getPredicatePartition(triple.getPredicate().toString());
                cnt = bf.estimatedCardinality();
                objects = bf.elementCount();
            }
            double multiplicity = (double) cnt / (double) distinct;
            if (!triple.getObject().equals("") && !triple.getObject().toString().startsWith("?")) {
                o = Double.min(o, 1.0 / (double) objects);
            } else if (vars.contains(triple.getObject().toString())) {
                //System.out.println("Bound var: " + triple.getObject() + " " + boundCount);
                o = Double.min(o, (1.0 / (double) objects) * (double) boundCount);
            } else {
                if (multiplicity == 0) multiplicity = 1;
                m = m * multiplicity;
            }
        }

        long card = (long) (distinct * m * o);
        if (card == 0) card = (long) (distinct * o);
        //if (card == 0) card = estimateCardReq(star, fragment);

        if (subjBound) {
            card = (long) ((double) card / (double) distinct);
        } else if (subjBoundVar) {
            card = (long) ((double) card / (double) distinct) * boundCount;
        }

        return card;
    }

    private long estimateCardReq(StarString star, IGraph fragment) {
        String url;
        try {
            url = getFragmentUrl(star, fragment);
        } catch (EncoderException e) {
            return 0;
        }

        return parseSize(url);
    }

    private long parseSize(String url) {
        Content content = null;
        try {
            content = Request.Get(url).addHeader("accept", "text/turtle").execute().returnContent();
        } catch (IOException e) {
            return 0;
        }
        //System.out.println(content.asString());
        TurtleParser parser = new TurtleParser(content.asStream());
        LothbrokTurtleEventHandler handler = new LothbrokTurtleEventHandler(url);
        parser.setEventHandler(handler);
        try {
            parser.parse();
        } catch (ParseException e) {
            return 0;
        }
        return handler.getSize();
    }

    private String getFragmentUrl(StarString star, IGraph graph) throws EncoderException {
        boolean isQuestionMarkAdded = false;
        StringBuilder sb = new StringBuilder();
        String address = AbstractNode.getState().getCommunity(graph.getCommunity()).getParticipant().getAddress();
        sb.append(address + (address.endsWith("/") ? "" : "/"));
        sb.append("ldf/" + graph.getId());

        int num = star.size();
        isQuestionMarkAdded = appendUrlParam(sb, getNode(star.getSubject().toString()), "subject", isQuestionMarkAdded);
        isQuestionMarkAdded = appendTripleParam(sb, num, "triples", isQuestionMarkAdded);

        String str = "[";
        for (int i = 0; i < num; i++) {
            Tuple<CharSequence, CharSequence> tpl = star.getTripleAt(i);
            int j = i + 1;
            str = str + "p" + j + "," + tpl.x.toString() + ";";
            str = str + "o" + j + "," + tpl.y.toString() + ";";
        }

        str = str.substring(0, str.length() - 1) + "]";
        isQuestionMarkAdded = appendStringParam(sb, str, "star", isQuestionMarkAdded);
        return sb.toString();
    }

    private static URLCodec urlCodec = new URLCodec("utf8");

    private boolean appendTripleParam(StringBuilder sb, int num, String paramName,
                                      Boolean isQuestionMarkAdded) throws EncoderException {
        sb.append("&").append(paramName).append("=").append(num);
        return isQuestionMarkAdded;
    }

    private boolean appendUrlParam(StringBuilder sb, Node node, String paramName,
                                   Boolean isQuestionMarkAdded) throws EncoderException {
        if (isQuestionMarkAdded) {
            if (node.isVariable())
                sb.append("&").append(paramName).append("=?").append(node.getName());
            else if (node.isLiteral())
                sb.append("&").append(paramName).append("=").append(urlCodec.encode("\"" + node.getLiteral().toString() + "\""));
            else
                sb.append("&").append(paramName).append("=").append(urlCodec.encode(node.getURI()));
        } else {
            if (node.isVariable()) {
                sb.append("?").append(paramName).append("=?").append(node.getName());
                return true;
            } else if (node.isLiteral()) {
                sb.append("?").append(paramName).append("=").append("\"" + urlCodec.encode(node.getLiteral().toString() + "\""));
                return true;
            } else {
                sb.append("?").append(paramName).append("=").append(urlCodec.encode(node.getURI()));
                return true;
            }
        }
        return isQuestionMarkAdded;
    }

    private boolean appendStringParam(StringBuilder sb, String str, String paramName,
                                      Boolean isQuestionMarkAdded) throws EncoderException {
        if (isQuestionMarkAdded) {
            sb.append("&").append(paramName).append("=").append(urlCodec.encode(str));
        } else {
            sb.append("?").append(paramName).append("=").append(urlCodec.encode(str));
            return true;
        }
        return isQuestionMarkAdded;
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

    @Override
    public Set<IGraph> getRelevantFragments(List<StarString> stars) {
        Set<IGraph> fragments = new HashSet<>();

        for (StarString star : stars) {
            fragments.addAll(getFragmentsByStar(star));
        }
        return fragments;
    }

    @Override
    public CompatibilityGraph getCompatibilityGraph(List<StarString> bgp) {
        StarString star = bgp.get(0);
        long lowestCardinality = estimateCardinality(star);
        for (int i = 1; i < bgp.size(); i++) {
            StarString st = bgp.get(i);
            long card = estimateCardinality(st);

            if (card < lowestCardinality) {
                lowestCardinality = card;
                star = st;
            }
        }

        List<StarString> bgp1 = new ArrayList<>(bgp);
        bgp1.remove(star);

        Set<StarString> visited = new HashSet<>();
        visited.add(star);
        CompatibilityGraph graph = new CompatibilityGraph();

        /*Set<IGraph> fragments = getFragmentsByStar(star);
        for (IGraph fragment : fragments) {
            if (fragment.identify(star) && bs.get(fragment).mightContainConstants(star)) {
                CompatibilityGraph graph1 = buildBranch(bgp1, fragment, star, visited, new HashMap<>());
                if (!graph1.isEmpty()) graph.addGraph(graph1);
            }
        }*/

        Map<IGraph, CompatibilityGraph> map = buildBranchMap(bgp1, star, visited);
        for (IGraph fragment : map.keySet()) {
            graph.addGraph(map.get(fragment));
        }

        List<StarString> nonContained = new ArrayList<>();
        for (StarString star1 : bgp1) {
            if (!visited.contains(star1)) {
                nonContained.add(star1);
            }
        }
        if (nonContained.size() > 0) {
            CompatibilityGraph graph1 = getCompatibilityGraph(nonContained);
            if (!graph1.isEmpty()) {
                for (IGraph fragment1 : graph.getFragments()) {
                    for (IGraph fragment2 : graph1.getFragments()) {
                        graph.addEdge(new CompatibleGraphs(fragment1, fragment2));
                    }
                }

                graph.addGraph(graph1);
            }
        }
        return graph;
    }

    private CompatibilityGraph buildBranch(List<StarString> bgp, IGraph curr, StarString star, Set<StarString> visited, Map<Long, CompatibilityGraph> visitedMap) {
        long hash = Objects.hash(curr, star);
        if (visitedMap.containsKey(hash)) return visitedMap.get(hash);
        CompatibilityGraph graph = new CompatibilityGraph();
        if (bgp.size() == 0 || !joins(bgp, star)) {
            graph.addFragment(curr);
            //System.out.println("Returning " + graph.toString());
            visitedMap.put(hash, graph);
            return graph;
        }
        for (StarString star1 : bgp) {
            if (!joins(star, star1)) continue;
            visited.add(star1);
            List<String> vars = star.getVariables();
            vars.retainAll(star1.getVariables());

            Set<IGraph> fragments = getFragmentsByStar(star1);
            for (IGraph fragment1 : fragments) {
                if (!fragment1.identify(star1) || !bs.get(fragment1).mightContainConstants(star1)) continue;
                boolean joins = true;
                for (String var : vars) {
                    Tuple<IGraph, IGraph> tpl1 = new Tuple<>(curr, fragment1);
                    Tuple<IGraph, IGraph> tpl2 = new Tuple<>(fragment1, curr);
                    IBloomFilter<String> intersection;
                    if (blooms.containsKey(new Tuple<>(tpl1, var))) {
                        intersection = blooms.get(new Tuple<>(tpl1, var));
                    } else if (blooms.containsKey(new Tuple<>(tpl2, var))) {
                        intersection = blooms.get(new Tuple<>(tpl2, var));
                    } else {
                        //System.out.println("            Finding intersection");
                        IBloomFilter<String> b1 = bs.get(curr).getPartition(star, var);
                        IBloomFilter<String> b2 = bs.get(fragment1).getPartition(star1, var);

                        String filename = curr.getId() + "-" + b1.getFileName().substring(b1.getFileName().lastIndexOf("/") + 1) + "--"
                                + fragment1.getId() + "-" + b2.getFileName().substring(b2.getFileName().lastIndexOf("/") + 1) + ".ppbf";

                        intersection = b1.intersect(b2, filename);
                        blooms.put(new Tuple<>(tpl1, var), intersection);
                    }

                    //System.out.println("            Intersection empty: " + intersection.isEmpty());

                    if (intersection.isEmpty()) {
                        joins = false;
                        break;
                    }
                }

                //System.out.println("            Joins " + joins);
                if (!joins) continue;
                List<StarString> bgp1 = new ArrayList<>(bgp);
                bgp1.remove(star1);

                CompatibilityGraph graph1 = buildBranch(bgp1, fragment1, star1, visited, visitedMap);
                graph.addGraph(graph1);
                graph.addFragments(curr, fragment1);
            }
        }
        //System.out.println("Returning " + graph.toString());
        visitedMap.put(hash, graph);
        return graph;
    }

    private Map<IGraph, CompatibilityGraph> buildBranchMap(List<StarString> bgp, StarString star, Set<StarString> visited) {
        Map<IGraph, CompatibilityGraph> map = new HashMap<>();
        Set<IGraph> fragments = getFragmentsByStar(star);
        if (bgp.size() == 0 || !joins(bgp, star)) {
            for (IGraph fragment : fragments) {
                CompatibilityGraph graph = new CompatibilityGraph();
                graph.addFragment(fragment);
                map.put(fragment, graph);
            }
            return map;
        }

        for (StarString star1 : bgp) {
            if (!joins(star, star1)) continue;
            visited.add(star1);
            List<String> vars = star.getVariables();
            vars.retainAll(star1.getVariables());

            List<StarString> bgp1 = new ArrayList<>(bgp);
            bgp1.remove(star1);
            Map<IGraph, CompatibilityGraph> map1 = buildBranchMap(bgp1, star1, visited);
            Set<IGraph> fragments1 = map1.keySet();
            for (IGraph fragment : fragments) {
                if (!fragment.identify(star) || !bs.get(fragment).mightContainConstants(star)) continue;
                CompatibilityGraph graph = new CompatibilityGraph();
                for (IGraph fragment1 : fragments1) {
                    if (!fragment1.identify(star1) || !bs.get(fragment1).mightContainConstants(star1)) continue;
                    boolean joins = true;

                    for (String var : vars) {
                        Tuple<IGraph, IGraph> tpl1 = new Tuple<>(fragment, fragment1);
                        Tuple<IGraph, IGraph> tpl2 = new Tuple<>(fragment1, fragment);
                        IBloomFilter<String> intersection;
                        if (blooms.containsKey(new Tuple<>(tpl1, var))) {
                            intersection = blooms.get(new Tuple<>(tpl1, var));
                        } else if (blooms.containsKey(new Tuple<>(tpl2, var))) {
                            intersection = blooms.get(new Tuple<>(tpl2, var));
                        } else {
                            //System.out.println("            Finding intersection");
                            IBloomFilter<String> b1 = bs.get(fragment).getPartition(star, var);
                            IBloomFilter<String> b2 = bs.get(fragment1).getPartition(star1, var);

                            String filename = fragment.getId() + "-" + b1.getFileName().substring(b1.getFileName().lastIndexOf("/") + 1) + "--"
                                    + fragment1.getId() + "-" + b2.getFileName().substring(b2.getFileName().lastIndexOf("/") + 1) + ".ppbf";

                            intersection = b1.intersect(b2, filename);
                            blooms.put(new Tuple<>(tpl1, var), intersection);
                        }

                        //System.out.println("            Intersection empty: " + intersection.isEmpty());

                        if (intersection.isEmpty()) {
                            joins = false;
                            break;
                        }
                    }

                    if (!joins) continue;
                    CompatibilityGraph graph1 = map1.get(fragment1);
                    graph.addGraph(graph1);
                    graph.addFragments(fragment, fragment1);
                }
                map.put(fragment, graph);
            }
        }
        return map;
    }

    private boolean joins(List<StarString> bgp, StarString star) {
        for (StarString star1 : bgp) {
            if (joins(star, star1)) return true;
        }
        return false;
    }

    private boolean joins(StarString star1, StarString star2) {
        List<String> vars = star1.getVariables();
        vars.retainAll(star2.getVariables());
        return vars.size() != 0;
    }

    @Override
    public IQueryStrategy getQueryStrategy(List<StarString> bgp, CompatibilityGraph graph) {
        DpTable table = new DpTable();
        for (StarString star : bgp) {
            List<IQueryStrategy> strategies = new ArrayList<>();
            Set<IGraph> fragments = getFragmentsByStar(star);
            fragments.retainAll(graph.getFragments());
            for (IGraph fragment : fragments) {
                if (!fragment.identify(star)) continue;
                strategies.add(QueryStrategyFactory.buildSingleStrategy(star, fragment));
            }
            List<StarString> subquery = new ArrayList<>();
            subquery.add(star);

            if (strategies.size() == 1)
                table.addEntry(new DpTableEntry(subquery, strategies.get(0), strategies.get(0).estimateCardinality(this)));
            else {
                IQueryStrategy strategy = QueryStrategyFactory.buildUnionStrategy(strategies);
                table.addEntry(new DpTableEntry(subquery, strategy, strategy.estimateCardinality(this)));
            }
        }
        if (bgp.size() == 1) return table.getTopmostStrategy();

        //System.out.println(table);
        return getSubStrategy(bgp, graph, table);
    }

    private IQueryStrategy getSubStrategy(List<StarString> bgp, CompatibilityGraph graph, DpTable table) {
        if (table.getTopmostEntry().isSubquery(bgp)) return table.getTopmostStrategy();

        List<DpTableEntry> topEntries = table.getTopEntries();
        for (StarString star : bgp) {
            List<StarString> lst = new ArrayList<>();
            lst.add(star);
            IQueryStrategy strategy = table.getStrategyBySubquery(lst);
            //System.out.println("Strategy: " + strategy.toString());
            for (DpTableEntry entry : topEntries) {
                if (entry.containsStar(star)) continue;

                //System.out.println(entry);
                IQueryStrategy strategy1 = QueryStrategyFactory.buildEmptyStrategy();
                if (strategy.getType() != QueryStrategyBase.Type.UNION || entry.getStrategy().getType() != QueryStrategyBase.Type.UNION) {
                    strategy1 = QueryStrategyFactory.buildJoinStrategy(entry.getStrategy(), strategy);
                } else {
                    Map<IQueryStrategy, List<IQueryStrategy>> strategyMap = new HashMap<>();
                    for (IQueryStrategy strat1 : ((UnionQueryStrategy) strategy).getStrategies()) {
                        List<IQueryStrategy> unions = new ArrayList<>();
                        IGraph f1 = strat1.getTopFragment();
                        for (IQueryStrategy strat2 : ((UnionQueryStrategy) entry.getStrategy()).getStrategies()) {
                            IGraph f2 = strat2.getTopFragment();
                            if (graph.hasEdge(f1, f2) || graph.hasEdge(f2, f1)) unions.add(strat2);
                        }
                        strategyMap.put(strat1, unions);
                    }

                    List<Tuple<List<IQueryStrategy>, List<IQueryStrategy>>> unions = new ArrayList<>();
                    for (Map.Entry<IQueryStrategy, List<IQueryStrategy>> e : strategyMap.entrySet()) {
                        boolean contains = false;
                        for (Tuple<List<IQueryStrategy>, List<IQueryStrategy>> tpl : unions) {
                            List<IQueryStrategy> str = new ArrayList<>(e.getValue());
                            str.retainAll(tpl.y);
                            if (!str.isEmpty()) {
                                contains = true;
                                tpl.x.add(e.getKey());
                                str = new ArrayList<>(e.getValue());
                                str.removeAll(tpl.y);
                                tpl.y.addAll(str);
                                break;
                            }
                        }
                        if (!contains) {
                            List<IQueryStrategy> x = new ArrayList<>();
                            x.add(e.getKey());
                            List<IQueryStrategy> y = new ArrayList<>(e.getValue());

                            unions.add(new Tuple<>(x, y));
                        }
                    }

                    if (unions.size() == 1) {
                        Tuple<List<IQueryStrategy>, List<IQueryStrategy>> tpl = unions.get(0);
                        IQueryStrategy right = getStrategyFromList(tpl.x);
                        IQueryStrategy left = getStrategyFromList(tpl.y);

                        strategy1 = QueryStrategyFactory.buildJoinStrategy(left, right);
                    } else {
                        List<IQueryStrategy> un = new ArrayList<>();
                        for (Tuple<List<IQueryStrategy>, List<IQueryStrategy>> tpl : unions) {
                            IQueryStrategy right = getStrategyFromList(tpl.x);
                            IQueryStrategy left = getStrategyFromList(tpl.y);
                            un.add(QueryStrategyFactory.buildJoinStrategy(left, right));
                        }
                        strategy1 = QueryStrategyFactory.buildUnionStrategy(un);
                    }
                }


                //System.out.println("Strategy1: " + strategy1.toString());
                List<StarString> subq = new ArrayList<>(entry.getSubquery());
                subq.add(star);

                //long cost = strategy1.estimateCardinality(this) + strategy.transferCost(AbstractNode.getState().getAsCommunityMember());
                long cost = strategy1.estimateCardinality(this) + entry.getCost();
                //System.out.println("Cost: " + cost);
                if (table.containsSubquery(subq)) {
                    if (cost < table.getEntryBySubquery(subq).getCost())
                        table.replaceStrategy(subq, new DpTableEntry(subq, strategy1, cost));
                } else {
                    table.addEntry(new DpTableEntry(subq, strategy1, cost));
                }
            }
        }
        //System.out.println();
        //System.out.println(table);
        return getSubStrategy(bgp, graph, table);
    }

    private Set<IGraph> getFragmentsByStar(StarString star) {
        List<String> preds = star.getPredicates();
        String first = preds.get(0);
        Set<IGraph> graphs;
        if (first.startsWith("?")) {
            graphs = new HashSet<>(fragments);
        } else {
            if (!fragmentMap.containsKey(first)) {
                System.out.println(first);
                //return new HashSet<>();
            }
            graphs = new HashSet<>(fragmentMap.get(first));
        }

        int size = preds.size();
        for (int i = 1; i < size; i++) {
            String pred = preds.get(i);
            if (pred.startsWith("?")) continue;
            if (!fragmentMap.containsKey(pred)) continue;
            graphs.retainAll(fragmentMap.get(pred));
        }
        return graphs;
    }

    private IQueryStrategy getStrategyFromList(List<IQueryStrategy> unions) {
        if (unions.size() == 1) return unions.get(0);
        else return QueryStrategyFactory.buildUnionStrategy(unions);
    }

    @Override
    public void addFragment(IGraph graph, IBloomFilter<String> filter) {
        IPartitionedBloomFilter<String> f = (IPartitionedBloomFilter<String>) filter;
        fragments.add(graph);
        bs.put(graph, f);

        ICharacteristicSet cs = graph.getCharacteristicSet();
        for (String pred : cs.getPredicates()) {
            if (fragmentMap.containsKey(pred)) fragmentMap.get(pred).add(graph);
            else {
                Set<IGraph> frags = new HashSet<>();
                frags.add(graph);
                fragmentMap.put(pred, frags);
            }
        }
    }

    @Override
    public void removeCommunity(String id) {
        System.out.println("removing indexes");
        Set<IGraph> keys = new HashSet<>(bs.keySet());

        for (IGraph key : keys) {
            if (key.isCommunity(id)) {
                IBloomFilter<String> f = bs.get(key);
                f.deleteFile();

                bs.remove(key);
                fragments.remove(key);

                ICharacteristicSet cs = key.getCharacteristicSet();
                for (String pred : cs.getPredicates()) {
                    if (fragmentMap.containsKey(pred)) fragmentMap.get(pred).remove(key);
                }
            }
        }

        Set<Tuple<Tuple<IGraph, IGraph>, String>> ks = new HashSet<>(blooms.keySet());
        for (Tuple<Tuple<IGraph, IGraph>, String> k : ks) {
            if (k.x.x.isCommunity(id) || k.x.y.isCommunity(id)) {
                IBloomFilter<String> f = blooms.get(k);
                f.deleteFile();
                blooms.remove(k);
            }
        }
    }

    @Override
    public Set<IGraph> getGraphs() {
        return fragments;
    }

    @Override
    public IGraph getGraph(String id) {
        for (IGraph graph : fragments) {
            if (graph.getId().equals(id)) return graph;
        }
        return null;
    }

    @Override
    public boolean hasFragment(String fid) {
        for (IGraph g : fragments) {
            if (g.getId().equals(fid)) return true;
        }
        return false;
    }

    @Override
    public void updateIndex(String fragmentId, IBloomFilter<String> filter) {
        IPartitionedBloomFilter<String> f = (IPartitionedBloomFilter<String>) filter;

        Set<IGraph> ks = bs.keySet();
        for (IGraph g : ks) {
            if (g.getId().equals(fragmentId)) bs.put(g, f);
        }

        Set<Tuple<Tuple<IGraph, IGraph>, String>> kks = blooms.keySet();
        for (Tuple<Tuple<IGraph, IGraph>, String> g : kks) {
            if (g.x.x.getId().equals(fragmentId) || g.x.y.getId().equals(fragmentId)) {
                blooms.get(g).deleteFile();
                blooms.remove(g);
            }
        }
    }

    @Override
    public String getPredicate(String id) {
        for (IGraph graph : fragments) {
            if (graph.getId().equals(id)) return graph.getBaseUri();
        }
        return "";
    }

    @Override
    public List<String> getByPredicate(String predicate) {
        List<String> ids = new ArrayList<>();

        for (IGraph g : fragments) {
            IPartitionedBloomFilter<String> filter = bs.get(g);
            if (filter.mightContainPredicate(predicate))
                ids.add(g.getId());
        }

        return ids;
    }
}
