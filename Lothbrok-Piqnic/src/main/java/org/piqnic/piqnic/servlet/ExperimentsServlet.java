package org.piqnic.piqnic.servlet;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.lothbrok.characteristicset.ICharacteristicSet;
import org.lothbrok.characteristicset.impl.CharacteristicSetFactory;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.index.graph.impl.Graph;
import org.lothbrok.index.ppbf.IBloomFilter;
import org.lothbrok.index.ppbf.impl.PrefixPartitionedBloomFilter;
import org.lothbrok.index.ppbf.impl.SemanticallyPartitionedBloomFilter;
import org.lothbrok.sparql.LothbrokJenaConstants;
import org.lothbrok.sparql.graph.LothbrokGraph;
import org.piqnic.piqnic.node.AbstractNode;
import org.piqnic.piqnic.node.INeighborNode;
import org.piqnic.piqnic.node.impl.NeighborNode;
import org.piqnic.piqnic.node.impl.NodeFactory;
import org.piqnic.piqnic.sparql.PiqnicJenaConstants;
import org.piqnic.piqnic.sparql.graph.PiqnicGraph;
import org.piqnic.piqnic.util.ConfigReader;
import org.piqnic.piqnic.util.MergedHDTIterator;
import org.piqnic.piqnic.util.RandomString;
import org.piqnic.piqnic.writer.IResponseWriter;
import org.piqnic.piqnic.writer.ResponseWriterFactory;
import org.apache.commons.io.FileUtils;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.lothbrok.utils.Tuple;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class ExperimentsServlet extends HttpServlet {
    private IResponseWriter writer = ResponseWriterFactory.createWriter();

    /**
     * @param servletConfig
     * @throws ServletException
     */
    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
    }

    /**
     *
     */
    @Override
    public void destroy() {
    }

    private void initConfig(String configName) throws ServletException {
        ConfigReader config;
        try {
            // load the configuration
            File configFile = new File(configName);
            config = new ConfigReader(new FileReader(configFile));
        } catch (Exception e) {
            throw new ServletException(e);
        }

        AbstractNode.getState().setDatastore(config.getLocalDatastore());
        AbstractNode.getState().setAddress(config.getAddress());
        File f = new File(config.getLocalDatastore() + "/hdt/");
        f.mkdirs();
        f = new File(config.getLocalDatastore() + "/index/");
        f.mkdirs();
    }


    /**
     * @param request
     * @param response
     * @throws ServletException
     */
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String mode = request.getParameter("mode");
        if (mode == null) return;

        try {
            if (mode.equals("setup")) handleSetup(request, response);
            else if (mode.equals("start")) handleStartLothbrok(request, response);
            else if (mode.equals("performance")) handlePerformance(request, response);
            else if (mode.equals("scalability")) handleScalability(request, response);
            else if (mode.equals("stress")) handleStress(request, response);
            else if (mode.equals("stress")) handleStress(request, response);
            else if (mode.equals("optimize")) handleOptimizer(request, response);
            else if (mode.equals("optimizeStress")) handleOptimizerStress(request, response);
            else if (mode.equals("optimizeLRB")) handleOptimizerLRB(request, response);

            writer.writeRedirect(response.getOutputStream(), request, "experiments");
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private void handleOptimizer(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String qDir = request.getParameter("queries");
        String oDir = request.getParameter("out");
        String load = request.getParameter("load");
        int nodes = Integer.parseInt(request.getParameter("nodes"));

        String queryDir = qDir + (qDir.endsWith("/")? "" : "/") + "client" + LothbrokJenaConstants.NODE + "/" + load;
        String outStr = oDir + (oDir.endsWith("/") ? "" : "/") + "optimizer/" + nodes + "/" + load + "/";
        File f1 = new File(outStr);
        f1.mkdirs();

        runOptimizer(queryDir, outStr);
    }

    private void handleOptimizerStress(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String qDir = request.getParameter("queries");
        String oDir = request.getParameter("out");
        int nodes = Integer.parseInt(request.getParameter("nodes"));

        String queryDir = qDir + (qDir.endsWith("/")? "" : "/") + "client" + LothbrokJenaConstants.NODE;
        String outStr = oDir + (oDir.endsWith("/") ? "" : "/") + "optimizer/" + nodes + "/sts/";
        File f1 = new File(outStr);
        f1.mkdirs();

        runOptimizer(queryDir, outStr);
    }

    private void handleOptimizerLRB(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String qDir = request.getParameter("queries");
        String oDir = request.getParameter("out");

        //String queryDir = qDir + (qDir.endsWith("/") ? "" : "/") + "client" + LothbrokJenaConstants.NODE + "/" + load;
        String outStr = oDir + (oDir.endsWith("/") ? "" : "/") + "optimizer/1/largerdfbench/";
        File f1 = new File(outStr);
        f1.mkdirs();

        runOptimizer(qDir, outStr);
    }

    private void runOptimizer(String queryDir, String outStr) throws IOException {
        LothbrokJenaConstants.NODES_INVOLVED = new HashSet<>();
        FileWriter out = new FileWriter(outStr + LothbrokJenaConstants.NODE + ".csv");
        File dir = new File(queryDir);
        File[] qFiles = dir.listFiles();

        System.out.println("Running optimization experiments...");

        for (File f : qFiles) {
            System.out.println(f.getName());
            String queryName = f.getName();

            String queryString = FileUtils.readFileToString(f, StandardCharsets.UTF_8);

            LothbrokJenaConstants.DISTINCT = queryString.contains("DISTINCT") || queryString.contains("distinct");

            final Duration timeout = Duration.ofMinutes(20);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            ExecutorCallableOptimizer callable = new ExecutorCallableOptimizer(queryString);

            final Future handler = executor.submit(callable);

            try {
                handler.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

                String str = queryName + ";" + callable.getOt() + ";"
                        + LothbrokJenaConstants.NRFBO + ";" + LothbrokJenaConstants.NRF + ";"
                        + LothbrokJenaConstants.NRNBO + ";" + LothbrokJenaConstants.NRN + ";"
                        + LothbrokJenaConstants.NIQ + ";" + LothbrokJenaConstants.NODES_INVOLVED.size() + ";"
                        + LothbrokJenaConstants.INDEXED + ";" + LothbrokJenaConstants.LOCAL;
                System.out.println(str);
                out.write(str + "\n");
            } catch (Exception e) {
                handler.cancel(true);
                callable.close();

                String str;
                if (e instanceof TimeoutException)
                    str = queryName + ";" + callable.getOt() + ";"
                            + LothbrokJenaConstants.NRFBO + ";" + LothbrokJenaConstants.NRF + ";"
                            + LothbrokJenaConstants.NRNBO + ";" + LothbrokJenaConstants.NRN + ";"
                            + LothbrokJenaConstants.NIQ + ";" + LothbrokJenaConstants.NODES_INVOLVED.size() + ";"
                            + LothbrokJenaConstants.INDEXED + ";" + LothbrokJenaConstants.LOCAL;
                else {
                    e.printStackTrace();
                    str = queryName + ";" + callable.getOt() + ";"
                            + LothbrokJenaConstants.NRFBO + ";" + LothbrokJenaConstants.NRF + ";"
                            + LothbrokJenaConstants.NRNBO + ";" + LothbrokJenaConstants.NRN + ";"
                            + LothbrokJenaConstants.NIQ + ";" + LothbrokJenaConstants.NODES_INVOLVED.size() + ";"
                            + LothbrokJenaConstants.INDEXED + ";" + LothbrokJenaConstants.LOCAL;
                }
                System.out.println(str);
                out.write(str + "\n");
            } finally {
                executor.shutdownNow();
            }
        }
        out.close();
    }

    private void handlePerformance(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String qDir = request.getParameter("queries");
        String oDir = request.getParameter("out");

        String outStr = oDir + (oDir.endsWith("/") ? "" : "/") + "performance/";
        File f1 = new File(outStr);
        f1.mkdirs();

        FileWriter out = new FileWriter(outStr + LothbrokJenaConstants.NODES + ".csv");
        File dir = new File(qDir);
        File[] qFiles = dir.listFiles();

        System.out.println("Running performance experiments...");

        for (File f : qFiles) {
            System.out.println(f.getName());
            String queryName = f.getName();

            String queryString = FileUtils.readFileToString(f, StandardCharsets.UTF_8);

            LothbrokJenaConstants.DISTINCT = queryString.contains("DISTINCT") || queryString.contains("distinct");

            final Duration timeout = Duration.ofMinutes(20);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            ExecutorCallable callable = new ExecutorCallable(queryString);

            final Future handler = executor.submit(callable);

            try {
                handler.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

                String str = queryName + ";" + callable.getQet() + ";" + callable.getRsp() + ";"
                        + LothbrokJenaConstants.NTB + ";" + LothbrokJenaConstants.NEM + ";"
                        + LothbrokJenaConstants.NRF + ";" + LothbrokJenaConstants.NRF + ";" + callable.getResults();
                System.out.println(str);
                out.write(str + "\n");
            } catch (Exception e) {
                handler.cancel(true);
                callable.close();

                String str = queryName + ";-1;-1;"
                        + LothbrokJenaConstants.NTB + ";" + LothbrokJenaConstants.NEM + ";"
                        + LothbrokJenaConstants.NRF + ";" + LothbrokJenaConstants.NRF + ";" + callable.getResults();
                System.out.println(str);
                out.write(str + "\n");
            } finally {
                executor.shutdownNow();
            }
        }
        out.close();
    }

    private void runScalability(String queryDir, String outStr) throws IOException {
        FileWriter out = new FileWriter(outStr + LothbrokJenaConstants.NODE + ".csv");
        File dir = new File(queryDir);
        File[] qFiles = dir.listFiles();

        System.out.println("Running scalability experiments...");

        for (File f : qFiles) {
            System.out.println(f.getName());
            String queryName = f.getName();

            String queryString = FileUtils.readFileToString(f, StandardCharsets.UTF_8);

            LothbrokJenaConstants.DISTINCT = queryString.contains("DISTINCT") || queryString.contains("distinct");

            final Duration timeout = Duration.ofMinutes(20);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            ExecutorCallable callable = new ExecutorCallable(queryString);

            final Future handler = executor.submit(callable);

            long start = System.currentTimeMillis();
            try {
                handler.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

                String str = queryName + ";" + callable.getQet() + ";" + callable.getRsp() + ";"
                        + LothbrokJenaConstants.NTB + ";" + LothbrokJenaConstants.NEM + ";"
                        + LothbrokJenaConstants.NRF + ";" + LothbrokJenaConstants.NRN + ";" + callable.getResults();
                System.out.println(str);
                out.write(str + "\n");
            } catch (Exception e) {
                handler.cancel(true);
                callable.close();

                String str;
                if(e instanceof TimeoutException)
                    str = queryName + ";-1;-1;"
                            + LothbrokJenaConstants.NTB + ";" + LothbrokJenaConstants.NEM + ";"
                            + LothbrokJenaConstants.NRF + ";" + LothbrokJenaConstants.NRN + ";" + callable.getResults();
                else {
                    e.printStackTrace();
                    long time = System.currentTimeMillis() - start;
                    str = queryName + ";" + time + ";" + time + ";"
                            + LothbrokJenaConstants.NTB + ";" + LothbrokJenaConstants.NEM + ";"
                            + LothbrokJenaConstants.NRF + ";" + LothbrokJenaConstants.NRN + ";" + callable.getResults();
                }
                System.out.println(str);
                out.write(str + "\n");
            } finally {
                executor.shutdownNow();
            }
        }
        out.close();
    }

    private void handleScalability(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String qDir = request.getParameter("queries");
        String oDir = request.getParameter("out");
        String load = request.getParameter("load");
        int nodes = Integer.parseInt(request.getParameter("nodes"));

        String queryDir = qDir + (qDir.endsWith("/")? "" : "/") + "client" + LothbrokJenaConstants.NODE + "/" + load;
        String outStr = oDir + (oDir.endsWith("/") ? "" : "/") + "scalability/" + nodes + "/" + load + "/";
        File f1 = new File(outStr);
        f1.mkdirs();

        runScalability(queryDir, outStr);
    }

    private void handleStress(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String qDir = request.getParameter("queries");
        String oDir = request.getParameter("out");
        int nodes = Integer.parseInt(request.getParameter("nodes"));

        String queryDir = qDir + (qDir.endsWith("/")? "" : "/") + "client" + LothbrokJenaConstants.NODE;
        String outStr = oDir + (oDir.endsWith("/") ? "" : "/") + "scalability/" + nodes + "/sts/";
        File f1 = new File(outStr);
        f1.mkdirs();

        runScalability(queryDir, outStr);
    }

    private void handleSetup(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String config = request.getParameter("config");
        initConfig(config);
        WebInterfaceServlet.INIT = true;

        int nodes = Integer.parseInt(request.getParameter("nodes"));
        int reps = Integer.parseInt(request.getParameter("rep"));

        String dirname = request.getParameter("dir");
        File dir = new File(dirname);
        if (!dir.exists() || !dir.isDirectory()) return;
        File[] fDirs = dir.listFiles();

        File oDir = new File("setup/");
        oDir.mkdirs();

        // Create Node IDs
        Gson gson = new Gson();
        List<String> ids = new ArrayList<>();
        RandomString rs = new RandomString(10);
        for (int i = 0; i < nodes; i++) {
            ids.add(rs.nextString());
        }
        FileWriter out = new FileWriter("setup/ids");
        out.write(gson.toJson(ids));
        out.close();

        // Create fragment distribution
        Random rand = new Random();
        Map<String, Tuple<Integer, Set<Integer>>> map = new HashMap<>();
        out = new FileWriter("setup/distribution");
        int j = 0;
        for (File fDir : fDirs) {
            System.out.println(j + "/" + fDirs.length);
            j++;
            if (!fDir.isDirectory()) continue;
            String cid = fDir.getName();
            int owner = -1;
            int num;
            if (reps == 0)
                num = (int) Math.ceil((double) nodes / (double) j);
            else
                num = reps;
            if (num == 0) num = 1;
            Set<Integer> set = new HashSet<>();
            while (set.size() < num) {
                int next = rand.nextInt(nodes);
                set.add(next);
                if (owner == -1) owner = next;
            }
            map.put(cid, new Tuple<>(owner, set));
        }

        out.write(gson.toJson(map));
        out.flush();
        out.close();
    }

    private void handleStartLothbrok(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException, FileNotFoundException {
        String config = request.getParameter("config");
        initConfig(config);
        WebInterfaceServlet.INIT = true;
        String setup = request.getParameter("setup");
        String dirname = request.getParameter("dir");
        int node = Integer.parseInt(request.getParameter("id"));
        int nodes = Integer.parseInt(request.getParameter("nodes"));
        LothbrokJenaConstants.NODES = nodes;
        LothbrokJenaConstants.NODE = node;
        AbstractNode.getState().setAddress("http://172.21.232.208:3" + String.format("%03d", node));
        setup = setup + (setup.endsWith("/") ? "" : "/");

        String file = setup + "distribution";
        String json = readFile(file);
        Gson gson = new Gson();
        Type type = new TypeToken<Map<String, Tuple<Integer, Set<Integer>>>>() {
        }.getType();
        Map<String, Tuple<Integer, Set<Integer>>> map = gson.fromJson(json, type);

        json = readFile(setup + "ids");
        type = new TypeToken<List<String>>() {
        }.getType();
        List<String> ids = gson.fromJson(json, type);
        try {
            AbstractNode.getState().setId(ids.get(node));
        } catch (IndexOutOfBoundsException e) {

        }

        int cnum = 1;

        for (String s : map.keySet()) {
            System.out.println("Handling fragment group " + s);
            Tuple<Integer, Set<Integer>> tpl = map.get(s);
            Set<Integer> set = tpl.y;

            Set<INeighborNode> storing = new HashSet<>();
            for (int i = 0; i < nodes; i++) {
                if (set.contains(i)) {
                    storing.add(NodeFactory.createNeighborNode("http://172.21.232.208:3" + String.format("%03d", i), ids.get(i)));
                }
            }
            boolean stores = false;
            if (set.contains(node)) {
                stores = true;
            }

            File[] fFiles = new File(dirname + (dirname.endsWith("/") ? "" : "/") + s).listFiles();
            for (File fFile : fFiles) {
                if (fFile.getName().contains(".index") || fFile.getName().contains(".chs")) continue;
                System.out.println("    Handling fragment " + fFile.getName());
                String path = dirname + (dirname.endsWith("/") ? "" : "/") + s + "/" + fFile.getName();

                String predFileName = fFile.getAbsolutePath().replace(".hdt", ".chs");
                ICharacteristicSet cs = getCSFromFilename(predFileName);
                String fid = fFile.getName().replace(".hdt", "");
                if (stores) {
                    AbstractNode.getState().addNewFragment(fid, fid, path, cs, storing);
                } else {
                    IGraph graph = new Graph(fid, fid, cs);
                    graph.addNodes(storing);
                    IBloomFilter<String> bloom = SemanticallyPartitionedBloomFilter.create(AbstractNode.getState().getDatastore() + "index/" + s);
                    AbstractNode.getState().getIndex().addFragment(graph, bloom);
                }
            }
        }
    }

    private ICharacteristicSet getCSFromFilename(String filename) throws FileNotFoundException {
        Set<String> preds = new HashSet<>();

        Scanner scanner = new Scanner(new File(filename));
        while (scanner.hasNextLine()) {
            String pred = scanner.nextLine();
            preds.add(pred);
        }

        return CharacteristicSetFactory.create(preds);
    }

    private static String readFile(String filePath) {
        StringBuilder contentBuilder = new StringBuilder();

        try (Stream<String> stream = Files.lines(Paths.get(filePath), StandardCharsets.UTF_8)) {
            stream.forEach(s -> contentBuilder.append(s).append("\n"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return contentBuilder.toString();
    }

    private static class ExecutorCallable implements Callable<String> {
        private final QueryExecution qExecutor;
        private long rsp = 0;
        private long qet = 0;
        private int results = 0;

        public ExecutorCallable(String qStr) {
            Query query = QueryFactory.create(qStr);
            final LothbrokGraph graph = new LothbrokGraph();
            Model model = ModelFactory.createModelForGraph(graph);
            qExecutor = QueryExecutionFactory.create(query, model);
        }

        public ExecutorCallable(String qStr, long timestamp) {
            Query query = QueryFactory.create(qStr);
            final LothbrokGraph graph = new LothbrokGraph(timestamp);
            Model model = ModelFactory.createModelForGraph(graph);
            qExecutor = QueryExecutionFactory.create(query, model);
        }

        public long getRsp() {
            return rsp;
        }

        public long getQet() {
            return qet;
        }

        public int getResults() {
            return results;
        }

        public void close() {
            qExecutor.abort();
        }

        @Override
        public String call() throws Exception {
            final ResultSet rs = qExecutor.execSelect();
            long start = System.currentTimeMillis();
            boolean first = true;

            while (rs.hasNext()) {
                rs.next();
                results++;
                if (first) {
                    rsp = System.currentTimeMillis() - start;
                    first = false;
                }
            }

            qet = System.currentTimeMillis() - start;
            return "";
        }
    }

    private class ExecutorCallableOptimizer implements Callable<Long> {
        private final QueryExecution qExecutor;
        private long ot = 0;
        private int results = 0;

        public ExecutorCallableOptimizer(String qStr) {
            Query query = QueryFactory.create(qStr);
            final LothbrokGraph graph = new LothbrokGraph();
            Model model = ModelFactory.createModelForGraph(graph);
            qExecutor = QueryExecutionFactory.create(query, model);
        }


        public long getOt() {
            return ot;
        }

        public void close() {
            qExecutor.abort();
        }

        @Override
        public Long call() throws Exception {
            long start = System.currentTimeMillis();
            final ResultSet rs = qExecutor.execSelect();
            ot = System.currentTimeMillis() - start;
            return ot;
        }
    }
}
