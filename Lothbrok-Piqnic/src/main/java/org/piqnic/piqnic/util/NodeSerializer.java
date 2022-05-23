package org.piqnic.piqnic.util;

import com.google.gson.*;
import org.piqnic.piqnic.node.INode;
import org.piqnic.piqnic.node.impl.NodeFactory;
import org.lothbrok.characteristicset.ICharacteristicSet;
import org.lothbrok.characteristicset.impl.CharacteristicSetFactory;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.index.graph.impl.Graph;
import org.lothbrok.index.index.IIndex;
import org.lothbrok.index.index.IPartitionedIndex;
import org.lothbrok.index.index.impl.PpbfIndex;
import org.lothbrok.index.index.impl.SpbfIndex;
import org.lothbrok.index.ppbf.IBloomFilter;
import org.lothbrok.index.ppbf.IPartitionedBloomFilter;
import org.lothbrok.index.ppbf.impl.PrefixPartitionedBloomFilter;
import org.lothbrok.index.ppbf.impl.SemanticallyPartitionedBloomFilter;
import org.lothbrok.utils.Tuple;

import java.lang.reflect.Type;
import java.util.*;

public class NodeSerializer implements JsonSerializer<INode>, JsonDeserializer<INode> {
    @Override
    public INode deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        JsonObject obj = jsonElement.getAsJsonObject();

        String id = obj.get("id").getAsString();
        String datastore = obj.get("datastore").getAsString();
        String address = obj.get("address").getAsString();
        IPartitionedIndex index = deserializePartitionedIndex(obj.get("index"));

        return NodeFactory.create(id, datastore, address, index);
    }

    @Override
    public JsonElement serialize(INode node, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject obj = new JsonObject();
        obj.addProperty("id", node.getId());
        obj.addProperty("datastore", node.getDatastore());
        obj.addProperty("address", node.getAddress());
        obj.add("index", serializePartitionedIndex((IPartitionedIndex) node.getIndex()));

        return obj;
    }

    private JsonElement serializePartitionedIndex(IPartitionedIndex index) {
        JsonObject obj = new JsonObject();

        JsonArray fArr = new JsonArray();
        Set<IGraph> graphs = index.getGraphs();
        for(IGraph graph : graphs) {
            fArr.add(serializeGraph(graph));
        }
        obj.add("fragments", fArr);

        JsonArray bArr = new JsonArray();
        Map<IGraph, IPartitionedBloomFilter<String>> bs = ((SpbfIndex) index).getBs();
        for(IGraph graph : bs.keySet()) {
            JsonObject o = new JsonObject();
            o.add("graph", serializeGraph(graph));
            o.addProperty("dir", (bs.get(graph)).getFileName());
            bArr.add(o);
        }
        obj.add("bs", bArr);

        JsonArray blArr = new JsonArray();
        Map<Tuple<Tuple<IGraph, IGraph>, String>, IBloomFilter<String>> blooms = ((SpbfIndex) index).getBlooms();
        for(Tuple<Tuple<IGraph, IGraph>, String> gs : blooms.keySet()) {
            JsonObject o = new JsonObject();
            o.add("graph1", serializeGraph(gs.x.x));
            o.add("graph2", serializeGraph(gs.x.y));
            o.addProperty("name", gs.y);
            o.addProperty("filename", (blooms.get(gs)).getFileName());
            blArr.add(o);
        }
        obj.add("blooms", blArr);

        return obj;
    }

    private IPartitionedIndex deserializePartitionedIndex(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();

        Set<IGraph> graphs = new HashSet<>();
        JsonArray fArr = obj.getAsJsonArray("fragments");
        for(int i = 0; i < fArr.size(); i++) {
            graphs.add(deserializeGraph(fArr.get(i)));
        }

        Map<IGraph, IPartitionedBloomFilter<String>> bs = new HashMap<>();
        JsonArray bArr = obj.getAsJsonArray("bs");
        for(int i = 0; i < bArr.size(); i++) {
            bs.put(deserializeGraph(bArr.get(i).getAsJsonObject().get("graph")),
                    SemanticallyPartitionedBloomFilter.create(bArr.get(i).getAsJsonObject().get("dir").getAsString()));
        }

        Map<Tuple<Tuple<IGraph, IGraph>, String>, IBloomFilter<String>> blooms = new HashMap<>();
        JsonArray blArr = obj.getAsJsonArray("blooms");
        for(int i = 0; i < blArr.size(); i++) {
            blooms.put(new Tuple<>(new Tuple<>(deserializeGraph(blArr.get(i).getAsJsonObject().get("graph1")), deserializeGraph(blArr.get(i).getAsJsonObject().get("graph2"))),
                            blArr.get(i).getAsJsonObject().get("name").getAsString()), PrefixPartitionedBloomFilter.create(blArr.get(i).getAsJsonObject().get("filename").getAsString() + ".ppbf"));
        }

        return new SpbfIndex(blooms, bs, graphs);
    }

    private JsonElement serializeIndex(IIndex index) {
        JsonObject obj = new JsonObject();

        JsonArray fArr = new JsonArray();
        Set<IGraph> graphs = index.getGraphs();
        for(IGraph graph : graphs) {
            fArr.add(serializeGraph(graph));
        }
        obj.add("fragments", fArr);

        JsonArray bArr = new JsonArray();
        Map<IGraph, IBloomFilter<String>> bs = ((PpbfIndex) index).getBs();
        for(IGraph graph : bs.keySet()) {
            JsonObject o = new JsonObject();
            o.add("graph", serializeGraph(graph));
            o.addProperty("filename", ((PrefixPartitionedBloomFilter)bs.get(graph)).getFilename());
            bArr.add(o);
        }
        obj.add("bs", bArr);

        JsonArray blArr = new JsonArray();
        Map<Tuple<IGraph, IGraph>, IBloomFilter<String>> blooms = ((PpbfIndex) index).getBlooms();
        for(Tuple<IGraph, IGraph> gs : blooms.keySet()) {
            JsonObject o = new JsonObject();
            o.add("graph1", serializeGraph(gs.x));
            o.add("graph2", serializeGraph(gs.y));
            o.addProperty("filename", ((PrefixPartitionedBloomFilter)blooms.get(gs)).getFilename());
            blArr.add(o);
        }
        obj.add("blooms", blArr);

        return obj;
    }

    private IIndex deserializeIndex(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();

        Set<IGraph> graphs = new HashSet<>();
        JsonArray fArr = obj.getAsJsonArray("fragments");
        for(int i = 0; i < fArr.size(); i++) {
            graphs.add(deserializeGraph(fArr.get(i)));
        }

        Map<IGraph, IBloomFilter<String>> bs = new HashMap<>();
        JsonArray bArr = obj.getAsJsonArray("bs");
        for(int i = 0; i < bArr.size(); i++) {
            bs.put(deserializeGraph(bArr.get(i).getAsJsonObject().get("graph")),
                    PrefixPartitionedBloomFilter.create(bArr.get(i).getAsJsonObject().get("filename").getAsString()));
        }

        Map<Tuple<IGraph, IGraph>, IBloomFilter<String>> blooms = new HashMap<>();
        JsonArray blArr = obj.getAsJsonArray("blooms");
        for(int i = 0; i < blArr.size(); i++) {
            blooms.put(new Tuple<>(deserializeGraph(blArr.get(i).getAsJsonObject().get("graph1")), deserializeGraph(blArr.get(i).getAsJsonObject().get("graph2"))),
                    PrefixPartitionedBloomFilter.create(blArr.get(i).getAsJsonObject().get("filename").getAsString()));
        }

        return new PpbfIndex(blooms, bs, graphs);
    }

    private JsonElement serializeGraph(IGraph graph) {
        JsonObject obj = new JsonObject();

        obj.addProperty("baseUri", graph.getBaseUri());
        obj.addProperty("id", graph.getId());
        obj.add("predicates", serializePredicates(graph.getCharacteristicSet()));

        return obj;
    }

    private JsonArray serializePredicates(ICharacteristicSet characteristicSet) {
        JsonArray arr = new JsonArray();
        List<String> preds = characteristicSet.getPredicates();

        for(String pred : preds) {
            arr.add(pred);
        }

        return arr;
    }

    private IGraph deserializeGraph(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();
        return new Graph(obj.get("baseUri").getAsString(), obj.get("id").getAsString(), deserializePredicates(obj.get("predicates").getAsJsonArray()));
    }

    private ICharacteristicSet deserializePredicates(JsonArray jsonArray) {
        Set<String> predicates = new HashSet<>();

        if(jsonArray != null && jsonArray.size() > 0) {
            for(int i = 0; i < jsonArray.size(); i++) {
                String pred = jsonArray.get(i).getAsString();
                predicates.add(pred);
            }
        }

        return CharacteristicSetFactory.create(predicates);
    }
}
