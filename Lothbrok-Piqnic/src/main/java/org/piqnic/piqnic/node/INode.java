package org.piqnic.piqnic.node;

import org.linkeddatafragments.datasource.IDataSource;
import org.lothbrok.characteristicset.ICharacteristicSet;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.index.index.IIndex;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public interface INode {
    IDataSource getDatasource(String id);
    Map<String, IDataSource> getDatasources();
    Set<String> getDatasourceIds();
    String getDatastore();
    void setDatastore(String datastore);
    void addNewFragment(String id, String predicate, String path, ICharacteristicSet cs);
    String getId();
    IIndex getIndex();
    String getAddress();
    void setAddress(String address);
    void saveState(String filename);
    void setId(String id);
    String getAddressPath();
    INeighborNode asNeighborNode();
    Set<INeighborNode> getNeighbors();
    void addNeighbor(INeighborNode node);
    int getNumRelevantNodes(Set<IGraph> fragments);
    int getNumLocallyStored(Set<IGraph> fragments);
    INeighborNode getAsNeighborNode();
    void addNewFragment(String id, String predicate, String path, ICharacteristicSet cs, Set<INeighborNode> nodes);
}
