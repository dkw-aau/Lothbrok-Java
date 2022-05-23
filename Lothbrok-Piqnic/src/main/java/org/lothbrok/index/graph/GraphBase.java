package org.lothbrok.index.graph;

import org.lothbrok.characteristicset.ICharacteristicSet;

public abstract class GraphBase implements IGraph {
    private final String baseUri;
    private final String id;
    private final ICharacteristicSet characteristicSet;

    public GraphBase(String baseUri, String id, ICharacteristicSet characteristicSet) {
        this.baseUri = baseUri;
        this.id = id;
        this.characteristicSet = characteristicSet;
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
                ", baseUri='" + baseUri + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
