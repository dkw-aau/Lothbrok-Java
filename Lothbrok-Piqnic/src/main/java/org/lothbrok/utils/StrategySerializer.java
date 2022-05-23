package org.lothbrok.utils;

import com.google.gson.*;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.IQueryStrategy;
import org.lothbrok.strategy.impl.*;
import org.piqnic.piqnic.node.AbstractNode;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class StrategySerializer implements JsonSerializer<IQueryStrategy>, JsonDeserializer<IQueryStrategy> {
    private final List<StarString> stars = new ArrayList<>();
    private boolean first = true;

    @Override
    public IQueryStrategy deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        return deserializeStrategy(jsonElement);
    }

    @Override
    public JsonElement serialize(IQueryStrategy strategy, Type type, JsonSerializationContext jsonSerializationContext) {
        return serializeStrategy(strategy);
    }

    private JsonElement serializeStrategy(IQueryStrategy strategy) {
        JsonObject obj = new JsonObject();

        if(first) {
            first = false;
            stars.addAll(strategy.getBGP());

            JsonArray jsonArray = new JsonArray();
            for(StarString star : stars) {
                jsonArray.add(serializeStar(star));
            }
            obj.add("stars", jsonArray);
        }

        if(strategy instanceof SingleQueryStrategy) serializeSingleStrategy((SingleQueryStrategy) strategy, obj);
        else if(strategy instanceof JoinQueryStrategy) serializeJoinStrategy((JoinQueryStrategy) strategy, obj);
        else if(strategy instanceof UnionQueryStrategy) serializeUnionStrategy((UnionQueryStrategy) strategy, obj);
        else serializeEmptyStrategy((EmptyQueryStrategy) strategy, obj);
        return obj;
    }

    private IQueryStrategy deserializeStrategy(JsonElement jsonElement) {
        JsonObject obj = jsonElement.getAsJsonObject();

        if(first) {
            first = false;
            JsonArray jsonArray = obj.getAsJsonArray("stars");
            for(JsonElement element : jsonArray) {
                stars.add(deserializeStar(element));
            }
        }

        String type = obj.get("type").getAsString();

        switch (type) {
            case "s":
                return deserializeSingleStrategy(jsonElement);
            case "j":
                return deserializeJoinStrategy(jsonElement);
            case "u":
                return deserializeUnionStrategy(jsonElement);
            default:
                return deserializeEmptyStrategy(jsonElement);
        }
    }

    private void serializeEmptyStrategy(EmptyQueryStrategy strategy, JsonObject obj) {
        obj.addProperty("type", "e");
    }

    private IQueryStrategy deserializeEmptyStrategy(JsonElement jsonElement) {
        return QueryStrategyFactory.buildEmptyStrategy();
    }

    private void serializeSingleStrategy(SingleQueryStrategy strategy, JsonObject obj) {
        obj.addProperty("star", stars.indexOf(strategy.getStar()));
        obj.addProperty("frag", strategy.getFragment().getId());
        obj.addProperty("type", "s");
    }

    private IQueryStrategy deserializeSingleStrategy(JsonElement jsonElement) {
        JsonObject obj = jsonElement.getAsJsonObject();
        IGraph fragment = AbstractNode.getState().getIndex().getGraph(obj.get("frag").getAsString());

        StarString star = stars.get(obj.get("star").getAsInt());
        return QueryStrategyFactory.buildSingleStrategy(star, fragment);
    }

    private void serializeJoinStrategy(JoinQueryStrategy strategy, JsonObject obj) {
        obj.addProperty("type", "j");
        obj.add("l", serializeStrategy(strategy.getLeft()));
        obj.add("r", serializeStrategy(strategy.getRight()));
    }

    private IQueryStrategy deserializeJoinStrategy(JsonElement jsonElement) {
        JsonObject obj = jsonElement.getAsJsonObject();
        return QueryStrategyFactory.buildJoinStrategy(deserializeStrategy(obj.get("l")), deserializeStrategy(obj.get("r")));
    }

    private void serializeUnionStrategy(UnionQueryStrategy strategy, JsonObject obj) {
        obj.addProperty("type", "u");
        JsonArray jsonArray = new JsonArray();
        for (IQueryStrategy strategy1 : strategy.getStrategies()) {
            jsonArray.add(serializeStrategy(strategy1));
        }
        obj.add("a", jsonArray);
    }

    private IQueryStrategy deserializeUnionStrategy(JsonElement jsonElement) {
        JsonObject obj = jsonElement.getAsJsonObject();
        List<IQueryStrategy> strategies = new ArrayList<>();
        JsonArray jsonArray = obj.getAsJsonArray("a");
        for(JsonElement element : jsonArray) {
            strategies.add(deserializeStrategy(element));
        }
        return QueryStrategyFactory.buildUnionStrategy(strategies);
    }

    private JsonElement serializeStar(StarString star) {
        JsonObject starObject = new JsonObject();
        starObject.addProperty("subj", star.getSubject().toString());
        for (Tuple<CharSequence, CharSequence> tpl : star.getTriples()) {
            starObject.addProperty(tpl.x.toString(), tpl.y.toString());
        }
        return starObject;
    }

    private StarString deserializeStar(JsonElement element) {
        JsonObject starObject = element.getAsJsonObject();
        String subj = "";
        List<Tuple<CharSequence, CharSequence>> tpls = new ArrayList<>();
        for (String key : starObject.keySet()) {
            if (key.equals("subj")) subj = starObject.get(key).getAsString();
            else {
                tpls.add(new Tuple<>(key, starObject.get(key).getAsString()));
            }
        }
        return new StarString(subj, tpls);
    }
}