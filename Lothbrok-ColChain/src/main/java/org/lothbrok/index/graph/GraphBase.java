package org.lothbrok.index.graph;

import org.lothbrok.characteristicset.ICharacteristicSet;
import org.lothbrok.characteristicset.impl.CharacteristicSetImpl;

public abstract class GraphBase implements IGraph {
    private final String community;
    private final String baseUri;
    private final String id;
    private final CharacteristicSetImpl characteristicSet;

    public GraphBase(String community, String baseUri, String id, ICharacteristicSet characteristicSet) {
        this.community = community;
        this.baseUri = baseUri;
        this.id = id;
        this.characteristicSet = (CharacteristicSetImpl) characteristicSet;
    }



    @Override
    public String getCommunity() {
        return community;
    }

    @Override
    public String getBaseUri() {
        return baseUri;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isCommunity(String id) {
        return community.equals(id);
    }

    @Override
    public int hashCode() {
        return baseUri.hashCode() + id.hashCode();
    }

    @Override
    public ICharacteristicSet getCharacteristicSet() {
        return characteristicSet;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj.getClass() != GraphBase.class) return false;
        GraphBase other = (GraphBase) obj;
        return baseUri.equals(other.baseUri) && id.equals(other.id);
    }

    @Override
    public String toString() {
        return "GraphBase{" +
                "community=" + community +
                ", baseUri='" + baseUri + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
