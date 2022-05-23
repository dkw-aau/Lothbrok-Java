package org.lothbrok.sparql;

import org.apache.jena.graph.Triple;
import org.apache.jena.n3.turtle.TurtleEventHandler;
import org.lothbrok.stars.StarString;
import org.lothbrok.utils.Tuple;

import java.util.*;

public class LothbrokTurtleEventHandler implements TurtleEventHandler {
    private final String fragmentUrl;
    private Map<String, String> prefix = new HashMap<>();
    private Map<String, StarString> starMap = new HashMap<>();
    private final Set<StarString> finished = new HashSet<>();
    private int size = 0;

    private Set<Triple> processedTriples = new HashSet<>();
    private String nextPageUrl = "";

    private static final int HYDRA_NEXTPAGE_HASH =
            new String("http://www.w3.org/ns/hydra/core#nextPage").hashCode();
    private static final int DATASET_HASH = new String("http://rdfs.org/ns/void#Dataset").hashCode();
    private static final int SUBSET_HASH = new String("http://rdfs.org/ns/void#subset").hashCode();
    private static final String SIZE_PRED = new String("http://www.w3.org/ns/hydra/core#totalItems");

    public LothbrokTurtleEventHandler(String fragmentUrl) {
        this.fragmentUrl = fragmentUrl;
    }

    @Override
    public void triple(int i, int i1, Triple triple) {
        //if(processedTriples.contains(triple)) return;
        //processedTriples.add(triple);
        if(triple.getPredicate().getURI().equals(SIZE_PRED)) {
            size = Integer.parseInt(triple.getObject().getLiteralLexicalForm());
            return;
        }

        if(isTripleValid(triple)) {
            String subjStr = triple.getSubject().isURI()? triple.getSubject().getURI() : triple.getSubject().getBlankNodeLabel();
            String predStr = triple.getPredicate().getURI();
            String objStr = triple.getObject().isURI()? triple.getObject().getURI() : (triple.getObject().isLiteral()? triple.getObject().getLiteral().toString() : triple.getObject().getBlankNodeLabel());
            if(!starMap.containsKey(subjStr))
                starMap.put(subjStr, new StarString(subjStr));
            else if(starMap.get(subjStr).getPredicates().contains(predStr)) {
                finished.add(starMap.get(subjStr));
                starMap.put(subjStr, new StarString(subjStr));
            }
            starMap.get(subjStr).addTriple(new Tuple<>(predStr, objStr));
        }
    }

    public int getSize() {
        return size;
    }

    public boolean hasNextPage() {
        return !nextPageUrl.equals("");
    }

    public String getNextPageUrl() {
        return nextPageUrl;
    }

    public List<StarString> getStars() {
        List<StarString> lst = new ArrayList<>(starMap.values());
        lst.addAll(finished);
        return lst;
    }

    private boolean isTripleValid(Triple triple) {
        if(triple.getSubject().isURI() && triple.getSubject().getURI().equals(fragmentUrl)) {
            if (triple.getPredicate().getURI().hashCode() == HYDRA_NEXTPAGE_HASH) {
                nextPageUrl = triple.getObject().getURI();
            }
            return false;
        } else if (triple.getPredicate().getURI().contains("hydra/")
                || (triple.getObject().isURI() && triple.getObject().getURI().contains("hydra/"))
                || (triple.getObject().isURI() && triple.getObject().getURI().hashCode() == DATASET_HASH)
                || triple.getPredicate().getURI().hashCode() == SUBSET_HASH) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void prefix(int i, int i1, String s, String s1) {
        prefix.put(s, s1);
    }

    @Override
    public void startFormula(int i, int i1) {
        // No idea
    }

    @Override
    public void endFormula(int i, int i1) {
        // No idea
    }
}
