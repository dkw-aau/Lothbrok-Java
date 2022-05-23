package org.lothbrok.utils;

import com.google.gson.*;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.reasoner.rulesys.Util;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.lothbrok.strategy.IQueryStrategy;
import org.lothbrok.strategy.impl.QueryStrategyFactory;

import java.lang.reflect.Type;
import java.util.*;

public class BindingSerializer implements JsonSerializer<List<Binding>>, JsonDeserializer<List<Binding>> {
    @Override
    public List<Binding> deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        JsonArray jsonArray = jsonElement.getAsJsonArray();

        List<Binding> bindings = new ArrayList<>();
        for(JsonElement element : jsonArray) {
            BindingHashMap binding = new BindingHashMap();
            JsonObject obj = element.getAsJsonObject();
            for (String s : obj.keySet()) {
                binding.add(Var.alloc(s), getNode(obj.get(s).getAsString()));
            }
            bindings.add(binding);
        }

        return bindings;
    }

    @Override
    public JsonElement serialize(List<Binding> bindings, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonArray jsonArray = new JsonArray();

        for(Binding binding : bindings) {
            JsonObject obj = new JsonObject();
            Iterator<Var> it = binding.vars();
            while (it.hasNext()) {
                Var v = it.next();
                String varname = v.getVarName().startsWith("?") ? v.getVarName() : "?" + v.getVarName();
                Node n = binding.get(v);
                String val;
                if (n.isLiteral()) val = n.getLiteralLexicalForm();
                else if (n.isBlank()) val = n.getBlankNodeLabel();
                else val = n.getURI();

                obj.add(varname, new JsonPrimitive(val));
            }
            jsonArray.add(obj);
        }
        return jsonArray;
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
