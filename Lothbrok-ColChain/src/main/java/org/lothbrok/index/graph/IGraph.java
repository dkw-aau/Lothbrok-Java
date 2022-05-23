package org.lothbrok.index.graph;

import org.lothbrok.characteristicset.ICharacteristicSet;
import org.lothbrok.stars.StarString;
import org.lothbrok.utils.Triple;

public interface IGraph {
    boolean identify(Triple triplePattern);
    boolean identify(StarString starPattern);
    boolean isCommunity(String id);
    String getCommunity();
    String getId();
    String getBaseUri();
    ICharacteristicSet getCharacteristicSet();
}
