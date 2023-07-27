package org.lothbrok.sparql.graph;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.ext.com.google.common.collect.Sets;
import org.apache.jena.graph.*;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.query.ARQ;
import org.apache.jena.reasoner.rulesys.Util;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.colchain.colchain.community.Community;
import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.sparql.ColchainJenaConstants;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.sparql.LothbrokBindings;
import org.lothbrok.sparql.LothbrokJenaConstants;
import org.lothbrok.sparql.iter.*;
import org.lothbrok.sparql.solver.LothbrokEngine;
import org.lothbrok.sparql.solver.OpExecutorLothbrok;
import org.lothbrok.sparql.solver.ReorderTransformationLothbrok;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.IQueryStrategy;
import org.lothbrok.strategy.impl.JoinQueryStrategy;
import org.lothbrok.strategy.impl.SingleQueryStrategy;
import org.lothbrok.strategy.impl.UnionQueryStrategy;
import org.lothbrok.utils.StrategySerializer;
import org.lothbrok.utils.Tuple;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class LothbrokGraph extends GraphBase {
    private static final Logger log = LoggerFactory.getLogger(LothbrokGraph.class);
    private LothbrokStatistics statistics = new LothbrokStatistics(this);
    private static LothbrokCapabilities capabilities = new LothbrokCapabilities();
    private ReorderTransformation reorderTransform;
    private static URLCodec urlCodec = new URLCodec("utf8");
    private final static Map<Tuple<String, Long>, HDT> FILE_CACHE = new HashMap<>();
    private long timestamp = 0;
    private boolean timeIncluded = false;

    static {
        QC.setFactory(ARQ.getContext(), OpExecutorLothbrok.opExecFactoryLothbrok);
        LothbrokEngine.register();
    }

    public LothbrokGraph() {
        LothbrokJenaConstants.NEM = 0;
        LothbrokJenaConstants.NTB = 0;
        LothbrokJenaConstants.NRN = 0;
        LothbrokJenaConstants.NRF = 0;
        reorderTransform = new ReorderTransformationLothbrok(this);
    }

    public LothbrokGraph(long timestamp) {
        LothbrokJenaConstants.NEM = 0;
        LothbrokJenaConstants.NTB = 0;
        LothbrokJenaConstants.NRN = 0;
        LothbrokJenaConstants.NRF = 0;
        reorderTransform = new ReorderTransformationLothbrok(this);
        this.timestamp = timestamp;
        this.timeIncluded = true;
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple jenaTriple) {
        throw new NotImplementedException("Not implemented for stars");
    }

    public ReorderTransformation getReorderTransform() {
        return reorderTransform;
    }

    public ExtendedIterator<Pair<StarString, Binding>> graphBaseFind(IQueryStrategy strategy, LothbrokBindings bindings) {
        if(strategy instanceof JoinQueryStrategy) return graphBaseFind((JoinQueryStrategy) strategy, bindings);
        else if(strategy instanceof UnionQueryStrategy) return graphBaseFind((UnionQueryStrategy) strategy, bindings);
        else if(strategy instanceof SingleQueryStrategy) return graphBaseFind((SingleQueryStrategy) strategy, bindings);
        else return new EmptyIterator();
    }

    public ExtendedIterator<Pair<StarString, Binding>> graphBaseFind(JoinQueryStrategy strategy, LothbrokBindings bindings) {
        //if(strategy.getNode().equals(AbstractNode.getState().getAsCommunityMember()))
            return new LocalLothbrokJoinIterator(strategy, bindings, this);

        /*String url;
        try {
            url = getSubqueryUrl(strategy, bindings);
        } catch (EncoderException e) {
            return new EmptyIterator();
        }
        return new RemoteLothbrokJoinIterator(url, strategy);*/
    }

    public ExtendedIterator<Pair<StarString, Binding>> graphBaseFind(UnionQueryStrategy strategy, LothbrokBindings bindings) {
        return new LocalLothbrokUnionIterator(strategy, this, bindings);
    }

    public ExtendedIterator<Pair<StarString, Binding>> graphBaseFind(SingleQueryStrategy strategy, LothbrokBindings bindings) {
        IGraph fragment = strategy.getFragment();
        String cid = fragment.getCommunity();
        Community community = AbstractNode.getState().getCommunity(cid);

        if(community.getMemberType() == Community.MemberType.PARTICIPANT) {
            if(timeIncluded)
                return new LocalLothbrokSingleIterator(strategy.getStar(), bindings, AbstractNode.getState().getDatasource(fragment.getId(), timestamp).getHdt());
            return new LocalLothbrokSingleIterator(strategy.getStar(), bindings, AbstractNode.getState().getDatasource(fragment.getId()).getHdt());
        }
        /*if(strategy.download()) {
            if ((timeIncluded && FILE_CACHE.containsKey(new Tuple<>(strategy.getFragment().getId() + ".hdt", timestamp))))
                return new LocalLothbrokSingleIterator(strategy.getStar(), bindings, FILE_CACHE.get(new Tuple<>(strategy.getFragment().getId() + ".hdt", timestamp)));
            if (FILE_CACHE.containsKey(new Tuple<>(strategy.getFragment().getId() + ".hdt", -1L)))
                return new LocalLothbrokSingleIterator(strategy.getStar(), bindings, FILE_CACHE.get(new Tuple<>(strategy.getFragment().getId() + ".hdt", -1L)));
            downloadFragment(strategy.getFragment());

            if(timeIncluded)
                return new LocalLothbrokSingleIterator(strategy.getStar(), bindings, FILE_CACHE.get(new Tuple<>(strategy.getFragment().getId() + ".hdt", timestamp)));
            else
                return new LocalLothbrokSingleIterator(strategy.getStar(), bindings, FILE_CACHE.get(new Tuple<>(strategy.getFragment().getId() + ".hdt", -1L)));
        }*/
        String url;

        try {
            url = getFragmentUrl(strategy.getStar(), bindings, fragment);
        } catch (EncoderException e) {
            return new EmptyIterator();
        }
        //System.out.println(url);
        return new RemoteLothbrokSingleIterator(url, strategy.getStar(), bindings);
    }

    private void downloadFragment(IGraph graph) {
        String url = getFragmentUrl(graph, false);
        String filePathString = AbstractNode.getState().getDatastore() + (AbstractNode.getState().getDatastore().endsWith("/") ? "" : "/") + "cache/";
        new File(filePathString).mkdirs();
        filePathString = filePathString + graph.getId() + (timeIncluded ? "-" + timestamp : "") + ".hdt";
        downloadFile(url, filePathString);

        // Download index
        url = getFragmentUrl(graph, true);
        filePathString = filePathString + ".index.v1-1";
        downloadFile(url, filePathString);

        try {
            if (timeIncluded)
                FILE_CACHE.put(new Tuple<>(graph.getId() + ".hdt", timestamp), HDTManager.mapIndexedHDT(filePathString.replace(".index.v1-1", "")));
            else
                FILE_CACHE.put(new Tuple<>(graph.getId() + ".hdt", -1L), HDTManager.mapIndexedHDT(filePathString.replace(".index.v1-1", "")));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void downloadFile(String url, String filePathString) {
        Content content = null;
        try {
            LothbrokJenaConstants.NEM++;
            content = Request.Get(url).addHeader("accept", "text/hdt").execute().returnContent();
            LothbrokJenaConstants.NTB += content.asBytes().length;
        } catch (IOException e) {
            return;
        }

        InputStream stream = content.asStream();
        try {
            Files.copy(stream, Paths.get(".", filePathString), StandardCopyOption.REPLACE_EXISTING);
            stream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getFragmentUrl(IGraph graph, boolean index) {
        StringBuilder sb = new StringBuilder();
        String address = AbstractNode.getState().getCommunity(graph.getCommunity()).getParticipant().getAddress();

        sb.append(address + (address.endsWith("/") ? "" : "/"));
        sb.append("fragment/" + graph.getId() + ".hdt");

        if (index) {
            sb.append(".index.v1-1");
        }

        if (timeIncluded) {
            sb.append("?");
            sb.append("time=" + timestamp);
        }

        return sb.toString();
    }

    private String getSubqueryUrl(JoinQueryStrategy strategy, LothbrokBindings bindings) throws EncoderException {
        boolean isQuestionMarkAdded = false;
        StringBuilder sb = new StringBuilder();
        String address = strategy.getNode().getAddress();
        sb.append(address).append(address.endsWith("/") ? "" : "/");
        sb.append("delegate");

        Gson gson = new GsonBuilder().registerTypeAdapter(IQueryStrategy.class, new StrategySerializer()).create();
        String str = gson.toJson(strategy, IQueryStrategy.class);
        isQuestionMarkAdded = appendStringParam(sb, str, "query", isQuestionMarkAdded);

        if (bindings.size() > 0) {
            appendBindings(strategy.getBGP(), sb, bindings);
        }

        if (timeIncluded) {
            isQuestionMarkAdded = appendStringParam(sb, "" + timestamp, "time", isQuestionMarkAdded);
        }
        return sb.toString();
    }

    private String getFragmentUrl(StarString star, LothbrokBindings bindings, IGraph graph) throws EncoderException {
        boolean isQuestionMarkAdded = false;
        StringBuilder sb = new StringBuilder();
        String address = AbstractNode.getState().getCommunity(graph.getCommunity()).getParticipant().getAddress();
        sb.append(address + (address.endsWith("/") ? "" : "/"));
        sb.append("ldf/" + graph.getId());

        int num = star.size();
        isQuestionMarkAdded = appendUrlParam(sb, getNode(star.getSubject().toString()), "subject", isQuestionMarkAdded);
        isQuestionMarkAdded = appendTripleParam(sb, num, "triples",isQuestionMarkAdded);

        String str = "[";
        for (int i = 0; i < num; i++) {
            Tuple<CharSequence, CharSequence> tpl = star.getTripleAt(i);
            int j = i + 1;
            str = str + "p"+j+"," + tpl.x.toString() + ";";
            str = str + "o"+j+"," + tpl.y.toString() + ";";
        }

        str = str.substring(0, str.length()-1) + "]";
        isQuestionMarkAdded = appendStringParam(sb, str, "star", isQuestionMarkAdded);
        if (bindings.size() > 0) {
            appendBindings(star, sb, bindings);
        }

        if (timeIncluded) {
            isQuestionMarkAdded = appendStringParam(sb, "" + timestamp, "time", isQuestionMarkAdded);
        }
        return sb.toString();
    }

    private boolean appendTripleParam(StringBuilder sb, int num, String paramName,
                                      Boolean isQuestionMarkAdded) throws EncoderException {
        sb.append("&").append(paramName).append("=").append(num);
        return isQuestionMarkAdded;
    }

    static String regex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
    private void appendBindings(StarString star, StringBuilder sb, LothbrokBindings bindings) throws EncoderException {
        if (bindings.size() > 0 && !bindings.get(0).isEmpty()) {
            Set<String> varsInSP = new HashSet<>();
            if(star.getSubject().charAt(0) == '?')
                varsInSP.add(star.getSubject().toString());

            for(Tuple<CharSequence, CharSequence> tpl : star.getTriples()) {
                if(tpl.x.charAt(0) == '?')
                    varsInSP.add(tpl.x.toString());
                if(tpl.y.charAt(0) == '?')
                    varsInSP.add(tpl.y.toString());
            }

            StringBuilder valuesSb = new StringBuilder();

            Set<String> boundVars = new HashSet<>();
            Iterator<Var> it = bindings.get(0).vars();
            while (it.hasNext()) {
                Var v = it.next();
                boundVars.add("?" + v.getVarName());
            }
            List<String> varsInURL = new ArrayList<>(varsInSP);
            varsInURL.retainAll(boundVars);
            if (varsInURL.size() == 0) return;

            valuesSb.append("(");
            String varStr = String.join(" ", varsInURL);

            valuesSb.append(varStr);
            valuesSb.append("){");


            Set<ArrayList<String>> set = new HashSet<>();
            for (int i = 0; i < bindings.size(); i++) {
                ArrayList<String> bindingsStrList = new ArrayList<String>();
                Binding binding = bindings.get(i);
                for (int j = 0; j < varsInURL.size(); j++) {
                    Iterator<Var> ii = binding.vars();
                    while (ii.hasNext()) {
                        Var v = ii.next();
                        String varname = "?" + v.getVarName();
                        if (varname.equals(varsInURL.get(j))) {
                            String str = binding.get(v).toString();
                            if(str.matches(regex))
                                bindingsStrList.add("<" + str + ">");
                            else
                                bindingsStrList.add("\"" + str + "\"");
                        }

                    }
                }
                if (set.contains(bindingsStrList)) continue;
                set.add(bindingsStrList);
                valuesSb.append("(");
                valuesSb.append(String.join(" ", bindingsStrList));
                valuesSb.append(")");
            }
            valuesSb.append("}");

            sb.append("&").append("values").append("=").append(urlCodec.encode(valuesSb.toString()));
        }
    }

    private void appendBindings(List<StarString> stars, StringBuilder sb, LothbrokBindings bindings) throws EncoderException {
        if (bindings.size() > 0 && !bindings.get(0).isEmpty()) {
            Set<String> varsInSP = new HashSet<>();
            for(StarString star : stars) {
                if (star.getSubject().charAt(0) == '?')
                    varsInSP.add("?" + star.getSubject().toString());

                for (Tuple<CharSequence, CharSequence> tpl : star.getTriples()) {
                    if (tpl.x.charAt(0) == '?')
                        varsInSP.add("?" + tpl.x.toString());
                    if (tpl.y.charAt(0) == '?')
                        varsInSP.add("?" + tpl.y.toString());
                }
            }

            StringBuilder valuesSb = new StringBuilder();

            Set<String> boundVars = new HashSet<>();
            Iterator<Var> it = bindings.get(0).vars();
            while (it.hasNext()) {
                Var v = it.next();
                boundVars.add("?" + v.getVarName());
            }
            ArrayList<String> varsInURL = new ArrayList<String>(Sets.intersection(varsInSP, boundVars));
            if (varsInURL.size() == 0) return;

            valuesSb.append("(");
            String varStr = String.join(" ", varsInURL);

            valuesSb.append(varStr);
            valuesSb.append("){");

            Set<ArrayList<String>> set = new HashSet<>();
            for (int i = 0; i < bindings.size(); i++) {
                ArrayList<String> bindingsStrList = new ArrayList<String>();
                Binding binding = bindings.get(i);
                for (int j = 0; j < varsInURL.size(); j++) {
                    Iterator<Var> ii = binding.vars();
                    while (ii.hasNext()) {
                        Var v = ii.next();
                        String varname = "?" + v.getVarName();
                        if (varname.equals(varsInURL.get(j)))
                            bindingsStrList.add("<" + binding.get(v).toString() + ">");
                    }
                }
                if (set.contains(bindingsStrList)) continue;
                set.add(bindingsStrList);
                valuesSb.append("(");
                valuesSb.append(String.join(" ", bindingsStrList));
                valuesSb.append(")");
            }
            valuesSb.append("}");
            sb.append("&").append("values").append("=").append(urlCodec.encode(valuesSb.toString()));
        }
    }

    private boolean appendUrlParam(StringBuilder sb, Node node, String paramName,
                                   Boolean isQuestionMarkAdded) throws EncoderException {
        if (isQuestionMarkAdded) {
            if (node.isVariable())
                sb.append("&").append(paramName).append("=?").append(node.getName());
            else if (node.isLiteral())
                sb.append("&").append(paramName).append("=").append(urlCodec.encode("\"" + node.getLiteral().toString() + "\""));
            else
                sb.append("&").append(paramName).append("=").append(urlCodec.encode(node.getURI()));
        } else {
            if (node.isVariable()) {
                sb.append("?").append(paramName).append("=?").append(node.getName());
                return true;
            } else if (node.isLiteral()) {
                sb.append("?").append(paramName).append("=").append("\"" + urlCodec.encode(node.getLiteral().toString() + "\""));
                return true;
            } else {
                sb.append("?").append(paramName).append("=").append(urlCodec.encode(node.getURI()));
                return true;
            }
        }
        return isQuestionMarkAdded;
    }

    private boolean appendStringParam(StringBuilder sb, String str, String paramName,
                                      Boolean isQuestionMarkAdded) throws EncoderException {
        if (isQuestionMarkAdded) {
            sb.append("&").append(paramName).append("=").append(urlCodec.encode(str));
        } else {
            sb.append("?").append(paramName).append("=").append(urlCodec.encode(str));
            return true;
        }
        return isQuestionMarkAdded;
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

    @Override
    public GraphStatisticsHandler getStatisticsHandler() {
        return statistics;
    }

    @Override
    public Capabilities getCapabilities() {
        return capabilities;
    }

    @Override
    protected int graphBaseSize() {
        //return (int)statistics.getStatistic(Node.ANY, Node.ANY, Node.ANY);
        return 1000000000;
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public String toString() {
        return "LothbrokGraph{" +
                "timestamp=" + timestamp +
                ", timeIncluded=" + timeIncluded +
                '}';
    }
}
