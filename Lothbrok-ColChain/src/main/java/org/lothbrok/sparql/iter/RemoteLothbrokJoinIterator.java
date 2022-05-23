package org.lothbrok.sparql.iter;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.reasoner.rulesys.Util;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.util.iterator.NiceIterator;
import org.lothbrok.sparql.LothbrokBindings;
import org.lothbrok.sparql.LothbrokJenaConstants;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.impl.JoinQueryStrategy;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

public class RemoteLothbrokJoinIterator extends NiceIterator<Pair<StarString, Binding>> {
    private final Gson gson = new Gson();

    private String currUrl;
    private final StarString star;
    private Queue<Binding> results = new LinkedList<>();
    private Binding next = null;

    public RemoteLothbrokJoinIterator(String currUrl, JoinQueryStrategy strategy) {
        this.currUrl = currUrl;
        this.star = strategy.getBGP().get(0);
    }

    private void bufferNext() {
        if (results.size() == 0) {
            if (currUrl == null) return;
            parseNext();
            bufferNext();
            return;
        }

        next = results.poll();
    }

    @Override
    public boolean hasNext() {
        if (next == null) bufferNext();
        return next != null;
    }

    @Override
    public Pair<StarString, Binding> next() {
        Pair<StarString, Binding> r = new Pair<>(star, next);
        next = null;
        return r;
    }

    private void parseNext() {
        if (currUrl == null || currUrl.equals("nil")) return;
        Content content = null;
        try {
            LothbrokJenaConstants.NEM++;
            content = Request.Get(currUrl).execute().returnContent();
            LothbrokJenaConstants.NTB += content.asBytes().length;
        } catch (IOException e) {
            currUrl = null;
            return;
        }
        String[] ws = content.asString().split("\n");

        Type typeToken = new TypeToken<ArrayList<HashMap<String, String>>>() { }.getType();
        List<Map<String, String>> maps = gson.fromJson(ws[0], typeToken);
        List<Binding> bindings = new ArrayList<>();
        for(Map<String, String> map : maps) {
            BindingHashMap binding = new BindingHashMap();
            for(String var : map.keySet()) {
                binding.add(Var.alloc(var.replace("?", "")), getNode(map.get(var)));
            }
            bindings.add(binding);
        }

        this.results = new LinkedList<>(bindings);
        if(!ws[1].startsWith("nil")) currUrl = ws[1];
        else currUrl = null;
    }

    private Node getNode(String element) {
        if (element.length() == 0) return NodeFactory.createBlankNode();
        char firstChar = element.charAt(0);
        if (firstChar == '_') {
            return NodeFactory.createBlankNode(element);
        } else if (firstChar == '"') {
            String noq = element.replace("\"", "");
            if (noq.matches("-?\\d+")) {
                return Util.makeIntNode(Integer.parseInt(noq));
            } else if (noq.matches("([0-9]+)\\.([0-9]+)")) {
                return Util.makeDoubleNode(Double.parseDouble(noq));
            }
            return NodeFactory.createLiteral(element.replace("\"", ""));
        } else {
            return NodeFactory.createURI(element);
        }
    }
}
