package org.piqnic.piqnic.writer;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import org.apache.commons.lang3.StringEscapeUtils;
import org.piqnic.piqnic.node.AbstractNode;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.linkeddatafragments.datasource.IDataSource;
import org.lothbrok.sparql.graph.LothbrokGraph;
import org.lothbrok.utils.Triple;
import org.lothbrok.utils.Tuple;
import org.piqnic.piqnic.node.INeighborNode;
import org.piqnic.piqnic.util.PiqnicConstants;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class ResponseWriterHtml implements IResponseWriter {
    private boolean error = false;

    private Template indexTemplate;
    private Template queryTemplate;
    private Template setupTemplate;
    private Template predicateTemplate;
    private Template detailsTemplate;

    protected ResponseWriterHtml() {
        final Configuration cfg = new Configuration(Configuration.VERSION_2_3_22);
        cfg.setClassForTemplateLoading(getClass(), "/views");
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

        try {
            indexTemplate = cfg.getTemplate("index.ftl.html");
            queryTemplate = cfg.getTemplate("query.ftl.html");
            setupTemplate = cfg.getTemplate("setup.ftl.html");
            predicateTemplate = cfg.getTemplate("predicate.ftl.html");
            detailsTemplate = cfg.getTemplate("details.ftl.html");
        } catch (IOException e) {
            System.out.println(e.getMessage());
            error = true;
        }
    }

    @Override
    public void writeNotInitiated(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        if (error) {
            outputStream.println("There was an error. Please restart the client.");
            return;
        }
        String uri = request.getRequestURI();
        if (uri.contains("ldf"))
            uri = uri.replace(uri.substring(uri.indexOf("ldf/")), "");
        else if (uri.contains("api"))
            uri = uri.replace(uri.substring(uri.indexOf("api/")), "");
        outputStream.println(
                "<!doctype html>\n" +
                        "<html>\n" +
                        "<head>\n" +
                        "<meta charset=\"utf-8\">\n" +
                        "<title>Not initiated</title>\n" +
                        "</head>\n" +
                        "<body>\n" +
                        "  The client has not been initiated! Please go to <a href=\"" + uri + "\">this link</a> to initiate. \n" +
                        "</body>\n" +
                        "</html>"
        );
    }

    @Override
    public void writeLandingPage(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        if (error) {
            outputStream.println("There was an error. Please restart the client.");
            return;
        }
        String uri = request.getRequestURI();
        if (!uri.endsWith("/"))
            uri += "/";

        Map data = new HashMap();

        data.put("assetsPath", "assets/");
        data.put("date", new Date());
        data.put("requestUri", uri);
        data.put("page", "Home");

        data.put("id", AbstractNode.getState().getId());
        data.put("address", AbstractNode.getState().getAddressPath());
        data.put("datastore", AbstractNode.getState().getDatastore());
        data.put("fragments", AbstractNode.getState().getDatasourceIds().size());
        data.put("ttl", PiqnicConstants.TTL);
        data.put("local", AbstractNode.getState().asNeighborNode());
        data.put("nodes", new ArrayList<INeighborNode>());
        data.put("neighbors", new ArrayList<Tuple<String, String>>());

        String date = "";
        boolean hasDate = false;
        if(request.getParameter("date") != null && !request.getParameter("date").equals("")) {
            date = request.getParameter("date");
            hasDate = true;
        }

        data.put("day", date);
        data.put("hasDate", hasDate);

        indexTemplate.process(data, new OutputStreamWriter(outputStream));
    }

    @Override
    public void writeRedirect(ServletOutputStream outputStream, HttpServletRequest request, String path) throws Exception {
        String uri = request.getRequestURI();
        uri = uri.substring(0, uri.indexOf(path));
        outputStream.println(
                "<!doctype html>\n" +
                        "<html>\n" +
                        "<head>\n" +
                        "<meta http-equiv=\"refresh\" content=\"0; url='" + uri + "\">\n" +
                        "</head>\n" +
                        "<body>\n" +
                        "</body>\n" +
                        "</html>"
        );
    }

    @Override
    public void writeInit(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        String uri = request.getRequestURI();

        Map data = new HashMap();

        data.put("assetsPath", "assets/");
        data.put("date", new Date());
        data.put("requestUri", uri);
        data.put("address", AbstractNode.getState().getAddressPath());
        data.put("page", "Setup");

        setupTemplate.process(data, new OutputStreamWriter(outputStream));
    }

    @Override
    public void writeQueryResults(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        if (error) {
            outputStream.println("There was an error. Please restart the client.");
            return;
        }
        String uri = request.getRequestURI().replace("api/sparql", "").replace("?", "");
        String sparql = request.getParameter("query");
        long timestamp = -1;
        String dateStr = "";
        if (request.getParameter("time") != null && !request.getParameter("time").equals(""))
            timestamp = Long.parseLong(request.getParameter("time"));
        if (request.getParameter("date") != null && !request.getParameter("date").equals("")) {
            dateStr = request.getParameter("date");
            DateTimeFormatter formatDateTime = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");
            LocalDateTime localDateTime = LocalDateTime.from(formatDateTime.parse(dateStr));
            Timestamp ts = Timestamp.valueOf(localDateTime);
            timestamp = ts.getTime();
        }

        Map data = new HashMap();

        data.put("assetsPath", "../assets/");
        data.put("date", new Date());
        data.put("requestUri", uri);
        data.put("address", AbstractNode.getState().getAddressPath());
        data.put("page", "Query result");
        data.put("query", sparql);
        data.put("timestamp", timestamp);
        data.put("dte", dateStr);

        Query query = QueryFactory.create(sparql);
        final LothbrokGraph graph;
        if (timestamp == -1)
            graph = new LothbrokGraph();
        else
            graph = new LothbrokGraph(timestamp);
        Model model = ModelFactory.createModelForGraph(graph);

        final QueryExecution executor = QueryExecutionFactory.create(query, model);
        final ResultSet rs = executor.execSelect();

        int cnt = 0;
        List<String> vars = new ArrayList<>();
        List<List<String>> values = new ArrayList<>();
        boolean first = true;
        while (rs.hasNext()) {
            QuerySolution sol = rs.next();
            cnt++;
            Iterator<String> vs = sol.varNames();
            List<String> vals = new ArrayList<>();
            while (vs.hasNext()) {
                String var = vs.next();
                if (first) {
                    vars.add(var);
                }
                vals.add(sol.get(var).toString());
            }
            values.add(vals);
            if (first) first = false;
        }

        data.put("count", cnt);
        data.put("variables", vars);
        data.put("values", values);

        queryTemplate.process(data, new OutputStreamWriter(outputStream));
    }

    @Override
    public void writeFragmentSearch(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        String uri = request.getRequestURI();
        String predicate = request.getParameter("predicate");

        Map data = new HashMap();

        data.put("assetsPath", "../assets/");
        data.put("date", new Date());
        data.put("requestUri", uri);
        data.put("address", AbstractNode.getState().getAddressPath());
        data.put("page", "Search: " + predicate);

        List<String> ids = AbstractNode.getState().getIndex().getByPredicate(predicate);
        List<FragmentMetadata> fragments = new ArrayList<>();
        for (String id : ids) {
            try {
                IDataSource datasource = AbstractNode.getState().getDatasource(id);
                fragments.add(new FragmentMetadata(id, datasource.numTriples(), datasource.numSubjects(),
                        datasource.numPredicates(), datasource.numObjects(), predicate));
            } catch (NullPointerException e) {
                continue;
            }
        }

        data.put("fragments", fragments);
        data.put("numFragments", fragments.size());

        predicateTemplate.process(data, new OutputStreamWriter(outputStream));
    }

    @Override
    public void writeFragmentDetails(ServletOutputStream outputStream, HttpServletRequest request) throws Exception {
        String uri = request.getRequestURI();
        String id = request.getParameter("id");

        Map data = new HashMap();

        data.put("assetsPath", "../assets/");
        data.put("date", new Date());
        data.put("requestUri", uri);
        data.put("address", AbstractNode.getState().getAddressPath());
        data.put("page", id);
        data.put("id", id);

        List<Triple> triples = new ArrayList<>();
        HDT hdt = AbstractNode.getState().getDatasource(request.getParameter("id")).getHdt();
        boolean search = request.getParameter("search") != null && !request.getParameter("search").equals("");
        if (!search) {
            IteratorTripleString iterator = hdt.search("", "", "");
            int count = 0;
            while(iterator.hasNext() && count < 20) {
                TripleString triple = iterator.next();
                triples.add(new Triple(triple.getSubject().toString(), triple.getPredicate().toString(), StringEscapeUtils.escapeHtml4(triple.getObject().toString())));
                count++;
            }
        } else {
            String s = request.getParameter("search");
            data.put("uri", s);

            // Subject position
            IteratorTripleString iterator = hdt.search(s, "", "");

            while(iterator.hasNext()) {
                TripleString triple = iterator.next();
                triples.add(new Triple(triple.getSubject().toString(), triple.getPredicate().toString(), StringEscapeUtils.escapeHtml4(triple.getObject().toString())));
            }

            // predicate position
            iterator = hdt.search("", s, "");

            while(iterator.hasNext()) {
                TripleString triple = iterator.next();
                triples.add(new Triple(triple.getSubject().toString(), triple.getPredicate().toString(), StringEscapeUtils.escapeHtml4(triple.getObject().toString())));
            }

            // Object position
            iterator = hdt.search("", "", s);

            while(iterator.hasNext()) {
                TripleString triple = iterator.next();
                triples.add(new Triple(triple.getSubject().toString(), triple.getPredicate().toString(), StringEscapeUtils.escapeHtml4(triple.getObject().toString())));
            }
        }

        data.put("isSearch", search);
        data.put("triples", triples);
        detailsTemplate.process(data, new OutputStreamWriter(outputStream));
    }

    public static class FragmentMetadata {
        public FragmentMetadata(String id, int triples, int subjects, int predicates, int objects, String predicate) {
            this.id = id;
            this.triples = triples;
            this.subjects = subjects;
            this.predicates = predicates;
            this.objects = objects;
            this.predicate = predicate;
        }

        private String predicate;
        private String id;
        private int triples;
        private int subjects;
        private int predicates;
        private int objects;

        public String getId() {
            return id;
        }

        public String getTriples() {
            return Integer.toString(triples);
        }

        public String getSubjects() {
            return Integer.toString(subjects);
        }

        public String getPredicates() {
            return Integer.toString(predicates);
        }

        public String getObjects() {
            return Integer.toString(objects);
        }

        public String getPredicate() {
            return predicate;
        }
    }
}
