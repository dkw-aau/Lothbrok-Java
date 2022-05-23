package org.colchain.colchain.node;

import org.colchain.colchain.community.Community;
import org.colchain.colchain.community.CommunityMember;
import org.colchain.colchain.knowledgechain.impl.ChainEntry;
import org.colchain.colchain.knowledgechain.impl.KnowledgeChain;
import org.colchain.colchain.transaction.ITransaction;
import org.linkeddatafragments.datasource.IDataSource;
import org.lothbrok.characteristicset.ICharacteristicSet;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.index.index.IIndex;
import org.lothbrok.utils.Tuple;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface INode {
    IDataSource getDatasource(String id);

    HashMap<String, IDataSource> getDatasources();

    Set<String> getDatasourceIds();

    String getDatastore();

    void setDatastore(String datastore);

    void addNewFragment(String id, String predicate, String path, String community, byte[] key, ICharacteristicSet cs);

    void addNewObservedFragment(String id, String predicate, String community, byte[] key, ICharacteristicSet cs);

    void setKeyPair(KeyPair keys);

    List<Community> getCommunities();

    void setCommunities(List<Community> communities);

    void leaveCommunity(String id);

    byte[] getPublicKey();

    String getId();

    void addCommunity(Community community);

    IIndex getIndex();

    String getAddress();

    void setAddress(String address);

    boolean hasCommunity(String id);

    Community getCommunity(String id);

    void addParticipant(CommunityMember m, String id);

    void addObserver(CommunityMember m, String id);

    void removeMember(CommunityMember m, String id);

    KnowledgeChain getChain(String id);

    void addChain(String id, KnowledgeChain chain);

    PrivateKey getPrivateKey();

    void suggestTransaction(ITransaction t, byte[] signature);

    void acceptTransaction(String tid, String nid);

    Community getCommunityByFragmentId(String id);

    void accept(ITransaction t);

    ITransaction getSuggestedTransaction(String id);

    List<ITransaction> getPendingUpdates();

    void saveState(String filename);

    KeyPair getKeys();

    Map<String, KnowledgeChain> getChains();

    Map<String, Tuple<ITransaction, Set<String>>> getTransactions();

    void setId(String id);

    void addNewFragment(String id, String predicate, String path, String community, byte[] key, ChainEntry entry, ICharacteristicSet cs);

    IDataSource getDatasource(String id, long timestamp);

    String getAddressPath();

    void addPending(Map<String, Tuple<ITransaction, Set<String>>> pending);

    int getNumRelevantNodes(Set<IGraph> fragments);

    int getNumLocallyStored(Set<IGraph> fragments);

    CommunityMember getAsCommunityMember();

    Set<CommunityMember> getNodesByFragments(Set<IGraph> fragments);
}
