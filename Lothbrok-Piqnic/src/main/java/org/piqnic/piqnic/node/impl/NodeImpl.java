package org.piqnic.piqnic.node.impl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.piqnic.piqnic.node.AbstractNode;
import org.piqnic.piqnic.node.INeighborNode;
import org.piqnic.piqnic.node.INode;
import org.piqnic.piqnic.util.NodeSerializer;
import org.piqnic.piqnic.util.RandomString;
import org.linkeddatafragments.datasource.DataSourceFactory;
import org.linkeddatafragments.datasource.IDataSource;
import org.lothbrok.characteristicset.ICharacteristicSet;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.index.graph.impl.Graph;
import org.lothbrok.index.index.IIndex;
import org.lothbrok.index.index.impl.SpbfIndex;
import org.lothbrok.index.ppbf.IPartitionedBloomFilter;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class NodeImpl extends AbstractNode implements INode {
    private String id;
    private IIndex index = new SpbfIndex();
    private final Map<String, IDataSource> datasources = new HashMap<>();
    private final Set<INeighborNode> neighbors = new HashSet<>();

    public NodeImpl(String id, String datastore, String address, IIndex index) {
        this.id = id;
        this.index = index;
        setDatastore(datastore);
        setAddress(address);
    }

    public NodeImpl() {
        RandomString gen = new RandomString();
        id = gen.nextString();
    }

    @Override
    public Set<INeighborNode> getNeighbors() {
        return neighbors;
    }

    @Override
    public void addNeighbor(INeighborNode node) {
        this.neighbors.add(node);
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public IDataSource getDatasource(String id) {
        if(!datasources.containsKey(id))
            return null;
        return datasources.get(id);
    }

    @Override
    public Map<String, IDataSource> getDatasources() {
        return datasources;
    }

    @Override
    public IIndex getIndex() {
        return index;
    }

    @Override
    public Set<String> getDatasourceIds() {
        return datasources.keySet();
    }

    @Override
    public void addNewFragment(String id, String predicate, String path, ICharacteristicSet cs) {
        IDataSource dataSource;
        try {
            dataSource = DataSourceFactory.createLocal(id, path);
        } catch (IOException e) {
            return;
        }
        datasources.put(id, dataSource);

        IGraph graph = new Graph(predicate, id, cs);
        graph.addNode(this.asNeighborNode());
        IPartitionedBloomFilter<String> filter = dataSource.createPartitionedBloomFilter();

        index.addFragment(graph, filter);
    }

    @Override
    public void addNewFragment(String id, String predicate, String path, ICharacteristicSet cs, Set<INeighborNode> nodes) {
        IDataSource dataSource;
        try {
            dataSource = DataSourceFactory.createLocal(id, path);
        } catch (IOException e) {
            return;
        }
        datasources.put(id, dataSource);

        IGraph graph = new Graph(predicate, id, cs);
        graph.addNodes(nodes);
        IPartitionedBloomFilter<String> filter = dataSource.createPartitionedBloomFilter();

        index.addFragment(graph, filter);
    }

    @Override
    public void saveState(String filename) {
        Gson gson = new GsonBuilder().registerTypeAdapter(INode.class, new NodeSerializer()).create();
        String json = gson.toJson(this, INode.class);

        try {
            PrintWriter writer = new PrintWriter(filename);
            writer.println(json);
            writer.flush();
            writer.close();
        } catch (IOException e) {}
    }

    @Override
    public int getNumRelevantNodes(Set<IGraph> fragments) {
        Set<INeighborNode> nodes = new HashSet<>();
        for(IGraph fragment : fragments) {
            nodes.addAll(fragment.getNeighbors());
        }
        return nodes.size();
    }

    @Override
    public int getNumLocallyStored(Set<IGraph> fragments) {
        int num = 0;
        for(IGraph fragment : fragments) {
            if(AbstractNode.getState().getDatasourceIds().contains(fragment.getId()))
                num++;
        }
        return num;
    }
}
