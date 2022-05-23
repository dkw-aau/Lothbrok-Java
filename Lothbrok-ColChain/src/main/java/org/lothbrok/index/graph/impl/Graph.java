package org.lothbrok.index.graph.impl;

import com.google.gson.Gson;
import org.lothbrok.characteristicset.ICharacteristicSet;
import org.lothbrok.index.graph.GraphBase;
import org.lothbrok.stars.StarString;
import org.lothbrok.utils.Triple;

import java.util.HashSet;

public class Graph extends GraphBase {
    public Graph(String community, String baseUri, String id, ICharacteristicSet characteristicSet) {
        super(community, baseUri, id, characteristicSet);
    }

    @Override
    public boolean identify(Triple triplePattern) {
        return triplePattern.getPredicate().equals("ANY")
                || triplePattern.getPredicate().startsWith("?")
                || triplePattern.getPredicate().equals(getBaseUri());
    }

    @Override
    public boolean identify(StarString starPattern) {
        ICharacteristicSet cs = getCharacteristicSet();
        for(String pred : starPattern.getPredicates()) {
            if(!pred.startsWith("?") && !cs.hasPredicate(pred)) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return getBaseUri().hashCode() + getId().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj.getClass() != getClass()) return false;
        Graph other = (Graph) obj;
        return getBaseUri().equals(other.getBaseUri()) && getId().equals(other.getId());
    }

    @Override
    public String toString() {
        return toJSONString();
    }

    private String toJSONString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
