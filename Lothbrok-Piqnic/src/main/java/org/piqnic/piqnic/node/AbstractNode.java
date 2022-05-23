package org.piqnic.piqnic.node;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.piqnic.piqnic.node.impl.NeighborNode;
import org.piqnic.piqnic.node.impl.NodeFactory;
import org.piqnic.piqnic.node.impl.NodeImpl;
import org.piqnic.piqnic.util.NodeSerializer;
import org.linkeddatafragments.datasource.IDataSource;
import org.lothbrok.characteristicset.ICharacteristicSet;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class AbstractNode implements INode {
    static INode state = NodeFactory.create();
    public static INode getState() {
        return state;
    }

    private String datastore = "";
    private String address = "";

    @Override
    public INeighborNode asNeighborNode() {
        return NodeFactory.createNeighborNode(address, this.getId());
    }

    @Override
    public String getDatastore() {
        return datastore;
    }

    @Override
    public void setDatastore(String datastore) {
        this.datastore = datastore;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public String getAddressPath() {
        if(address.endsWith("/")) return address;
        return address + "/";
    }

    @Override
    public void setAddress(String address) {
        this.address = address;
    }

    public abstract IDataSource getDatasource(String id);
    public abstract Map<String, IDataSource> getDatasources();
    public abstract Set<String> getDatasourceIds();

    public static void loadState(String filename) {
        StringBuilder sb = new StringBuilder();

        try {
            InputStream is = new FileInputStream(filename);
            BufferedReader in = new BufferedReader(new InputStreamReader(is));

            String line = in.readLine();
            while(line != null) {
                sb.append(line);
                line = in.readLine();
            }
        } catch (IOException e) {
            return;
        }

        String json = sb.toString();

        Gson gson = new GsonBuilder().registerTypeAdapter(INode.class, new NodeSerializer()).create();
        state = gson.fromJson(json, INode.class);
    }

    @Override
    public INeighborNode getAsNeighborNode() {
        return NodeFactory.createNeighborNode(address, getId());
    }
}
