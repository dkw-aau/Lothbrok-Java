package org.colchain.colchain.servlet;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.tokens.Tokenizer;
import org.apache.jena.riot.tokens.TokenizerText;
import org.apache.jena.tdb.store.Hash;
import org.colchain.colchain.community.Community;
import org.colchain.colchain.community.CommunityMember;
import org.colchain.colchain.knowledgechain.impl.ChainEntry;
import org.colchain.colchain.knowledgechain.impl.KnowledgeChain;
import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.sparql.ColchainJenaConstants;
import org.colchain.colchain.sparql.graph.ColchainGraph;
import org.colchain.colchain.transaction.ITransaction;
import org.colchain.colchain.transaction.Operation;
import org.colchain.colchain.util.*;
import org.colchain.colchain.writer.IResponseWriter;
import org.colchain.colchain.writer.ResponseWriterFactory;
import org.apache.commons.io.FileUtils;
import org.apache.jena.query.*;
import org.lothbrok.characteristicset.ICharacteristicSet;
import org.lothbrok.characteristicset.impl.CharacteristicSetFactory;
import org.lothbrok.sparql.LothbrokJenaConstants;
import org.lothbrok.sparql.graph.LothbrokGraph;
import org.lothbrok.utils.FragmentGeneratorIterator;
import org.lothbrok.utils.Triple;
import org.lothbrok.utils.Tuple;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.iterator.utils.MergedIterator;
import org.rdfhdt.hdt.listener.ProgressOut;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorStarString;
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
            else if (mode.equals("fragments")) handleCreateFragmentsStar(request, response);
            else if (mode.equals("hdt")) handleHdt(request, response);
            else if (mode.equals("community")) handleCommunity(request, response);
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

    private void handleCommunity(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException, NotFoundException, ParserException {
        String oDir = request.getParameter("out");
        String dDir = request.getParameter("data");

        createCommunitiesFromDirectory(dDir, oDir);
    }

    private void handleHdt(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException, NotFoundException, ParserException {
        String oDir = request.getParameter("out");
        String dDir = request.getParameter("data");
        String cDir = request.getParameter("communities");

        createFragmentsFromFile(dDir, oDir, cDir);
    }

    private void handleCreateFragmentsStar(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException, NotFoundException, ParserException {
        String oDir = request.getParameter("out");
        String dDir = request.getParameter("data");

        createFragmentsFromDir(dDir, oDir);

        /*long count = 0;
        System.out.println("Finding Obama");
        File[] cDirs = new File(dDir).listFiles();
        for(File cDir : cDirs) {
            File[] fFiles = cDir.listFiles();
            for(File fFile : fFiles) {
                if(fFile.getName().contains(".chs") || fFile.getName().contains(".index") || fFile.getName().contains("fragments")) continue;

                /*Set<String> preds = new HashSet<>();
                Scanner scanner = new Scanner(new File(fFile.getAbsolutePath().replace(".hdt", ".chs")));
                while(scanner.hasNextLine()) {
                    String pred = scanner.nextLine();
                    preds.add(pred);
                }*/

        //if(preds.contains("http://data.nytimes.com/elements/topicPage") && preds.contains("http://www.w3.org/2002/07/owl#sameAs")) {
        //if(preds.contains("http://www.w3.org/2002/07/owl#sameAs")) {

                /*    HDT hdt = HDTManager.mapHDT(fFile.getAbsolutePath());
                    DictionarySection section = hdt.getDictionary().getPredicates();
                    String pred;
                    try {
                        Iterator<? extends CharSequence> it1 = section.getSortedEntries();
                        if (it1 == null || !it1.hasNext()) continue;

                        pred = it1.next().toString();
                    } catch (NullPointerException e) {
                        continue;
                    }
                    if(pred.equals("http://www.w3.org/2002/07/owl#sameAs")) {
                        System.out.println("Found fragment: " + cDir + "/" + fFile.getName());
                        IteratorTripleString it = hdt.search("", "", "");

                        while (it.hasNext()) {
                            TripleString ts = it.next();
                            if (ts.getSubject().toString().equals("http://data.nytimes.com/47452218948077706853")) {
                                System.out.println("Found triple: " + ts.toString() + " in fragment: " + fFile.getName());
                            }
                        }
                    }
               //}
            }
        }

        System.out.println(count);*/

        /*System.out.println("Creating star fragments");
        File[] fDirs = new File(dDir).listFiles();
        for (File fFile : fDirs) {
            System.out.println("Creating for file " + fFile.getName());
            HDT hdt = HDTManager.loadHDT(fFile.getAbsolutePath());
            createFragmentsFromHDT(hdt, oDir);
        }*/

        //HDT hdt = HDTManager.loadHDT(dDir);
        //createFragmentsFromHDT(hdt, oDir);

        //HDT hdt = HDTManager.loadHDT("data/eug9eVF5mq.hdt");
        //createFragmentsFromHDT(hdt, "data_stars");

        /*long card = 0;
        MergedHDTIterator<TripleString> it = new MergedHDTIterator<>();
        File[] fDirs = new File(dDir).listFiles();
        for (File fDir : fDirs) {
            System.out.println("For dir: " + fDir.getName());
            File[] fFiles = fDir.listFiles();
            for (File fFile : fFiles) {
                System.out.println("    For file: " + fFile.getName());
                if (fFile.getName().contains("fragments") || fFile.getName().contains(".index")) continue;
                HDT hdt = HDTManager.loadHDT(fFile.getAbsolutePath());
                IteratorTripleString it1 = hdt.search("", "", "");
                card += it1.estimatedNumResults();
                it.addIterator(it1);
            }
        }

        System.out.println("Saving HDT. Found " + card + " triples.");
        HDT hdt = HDTManager.generateHDT(it, "http://relweb.cs.aau.dk/colchain#fragments", new HDTSpecification(), ProgressOut.getInstance());
        hdt.saveToHDT(oDir + "/merged.hdt", ProgressOut.getInstance());

        System.out.println("HDT saved. Generating star fragments.");

        createFragmentsFromHDT(hdt, oDir);
        System.out.println("Done!");*/
    }

    void createCommunitiesFromDirectory(String dDir, String oDir) {
        System.out.println("Creating the communities...");
        RandomString gen = new RandomString(10);
        Random rand = new Random();
        List<String> ids = new ArrayList<>();

        for (int i = 0; i < 200; i++) {
            String id = gen.nextString();
            new File(oDir + "/" + id).mkdir();
            ids.add(id);
        }

        int num = 1;
        File[] fFiles = new File(dDir).listFiles();
        for (File fFile : fFiles) {
            if (fFile.getName().contains(".index") || fFile.getName().contains(".chs")) continue;
            int i = rand.nextInt(ids.size());
            String id = ids.get(i);
            System.out.print("\rMoving fragment " + fFile.getName() + " to community " + id + " (" + num + ").");
            num++;
            fFile.renameTo(new File(oDir + "/" + id + "/" + fFile.getName()));
            new File(fFile.getAbsolutePath().replace(".hdt", ".chs"))
                    .renameTo(new File(oDir + "/" + id + "/" + fFile.getName().replace(".hdt", ".chs")));
        }
        System.out.print("\n");
        System.out.println("Done.");
    }

    void createFragmentsFromDir(String dDir, String oDir) throws IOException, NotFoundException {
        int threshold = 100;
        Map<String, Set<Tuple<String, String>>> subjMap = new HashMap<>();
        Map<String, HDT> hdtMap = new HashMap<>();
        RandomString gen = new RandomString(10);

        System.out.println("Reading already create characteristic sets.");
        Set<ICharacteristicSet> sets = readCharacteristicSets(oDir);
        System.out.println("Found " + sets.size() + " characteristic sets.");

        File[] fDirs = new File(dDir).listFiles();
        for (File fDir : fDirs) {
            System.out.println("Handling directory " + fDir.getName());
            File[] fFiles = fDir.listFiles();
            for (File fFile : fFiles) {
                if (fFile.getName().contains("fragments") || fFile.getName().contains(".index")) continue;
                System.out.println("    Handling file " + fFile.getName());

                HDT hdt = HDTManager.loadHDT(fFile.getAbsolutePath());
                hdtMap.put(fFile.getAbsolutePath(), hdt);
                String pred = hdt.getDictionary().getPredicates().getSortedEntries().next().toString();

                if (fFile.getName().equals("uhSq6OB0qQ.hdt")) {
                    System.out.println("Found Obama file");
                }

                IteratorTripleString iterator = hdt.search("", "", "");
                //DictionarySection section = hdt.getDictionary().getSubjects();
                //Iterator<? extends CharSequence> iterator = section.getSortedEntries();
                while (iterator.hasNext()) {
                    String subj = iterator.next().getSubject().toString();
                    if (subj.equals("http://data.nytimes.com/47452218948077706853"))
                        System.out.println("http://data.nytimes.com/47452218948077706853: " + pred);
                    if (subjMap.containsKey(subj)) subjMap.get(subj).add(new Tuple<>(pred, fFile.getAbsolutePath()));
                    else {
                        Set<Tuple<String, String>> set = new HashSet<>();
                        set.add(new Tuple<>(pred, fFile.getAbsolutePath()));
                        subjMap.put(subj, set);
                    }
                }
            }
        }

        System.out.println("Predicates for Obama: " + subjMap.get("http://data.nytimes.com/47452218948077706853").toString());

        System.out.println("Found " + subjMap.size() + " subjects.");
        System.out.println("Finding characteristic sets...");
        Set<ICharacteristicSet> csSet = new HashSet<>();
        Map<ICharacteristicSet, Set<String>> csMap = new HashMap<>();

        ICharacteristicSet obamaCs = null;

        for (String subj : subjMap.keySet()) {
            Set<String> sset = new HashSet<>();
            for (Tuple<String, String> tpl : subjMap.get(subj)) {
                sset.add(tpl.x);
            }

            ICharacteristicSet csSet1 = CharacteristicSetFactory.create(sset);
            csSet.add(csSet1);

            if (csMap.containsKey(csSet1)) csMap.get(csSet1).add(subj);
            else {
                Set<String> ssset = new HashSet<>();
                ssset.add(subj);
                csMap.put(csSet1, ssset);
            }

            if (subj.equals("http://data.nytimes.com/47452218948077706853")) {
                obamaCs = csSet1;
                System.out.println("Obama CS: " + csSet1);
            }
        }

        System.out.println("Found " + csSet.size() + " characteristic sets.");
        System.out.println("Pruning subsets...");
        Set<ICharacteristicSet> cSets = new HashSet<>(csSet);
        int pruned = 0;

        for (ICharacteristicSet cSet : cSets) {
            if (!csMap.containsKey(cSet)) continue;

            ICharacteristicSet superCs = getSuperset(cSet, csMap.keySet());
            if (superCs == null) continue;

            csMap.get(superCs).addAll(csMap.get(cSet));
            csMap.remove(cSet);
            csSet.remove(cSet);

            pruned++;
            System.out.print("\rPruned " + pruned + " characteristic sets.");
        }

        System.out.print("\n");


        System.out.println("Finding obama's CSs");
        for (ICharacteristicSet cs : csSet) {
            if (csMap.get(cs).contains("http://data.nytimes.com/47452218948077706853")) {
                System.out.println("Found Obama in CS: " + cs.toString());
            }
        }
        System.out.println("Done searching for Obama.");


        System.out.println("Pruning infrequent characteristic sets...");
        cSets = new HashSet<>(csSet);
        pruned = 0;

        for (ICharacteristicSet cSet : cSets) {
            if (csMap.get(cSet).size() >= threshold) continue;

            Set<String> remainingPreds = new HashSet<>(cSet.getPredicates());
            while (remainingPreds.size() > 0) {
                ICharacteristicSet largest = getLargest(remainingPreds, csSet);
                if (largest == null) {
                    break;
                }

                if (csMap.get(cSet).contains("http://data.nytimes.com/47452218948077706853")) {
                    System.out.println("Pruned Obama. New CS: " + largest.toString());
                }

                csMap.get(largest).addAll(csMap.get(cSet));
                remainingPreds.removeAll(largest.getPredicates());
            }


            csMap.remove(cSet);
            csSet.remove(cSet);

            pruned++;
            System.out.print("\rPruned " + pruned + " characteristic sets.");
        }

        System.out.print("\n");
        System.out.println("Left with " + csSet.size() + " characteristic sets.");


        System.out.println("Finding obama's CSs");
        for (ICharacteristicSet cs : csSet) {
            if (csMap.get(cs).contains("http://data.nytimes.com/47452218948077706853")) {
                System.out.println("Found Obama in CS: " + cs.toString());
            }
        }
        System.out.println("Done searching for Obama.");


        for (ICharacteristicSet cs : csSet) {
            if (sets.contains(cs)) {
                System.out.println("    Already created the CS. Skipping.");
                continue;
            }

            System.out.println("    Handling CS " + cs.toString());

            String id = gen.nextString();
            String outpath = oDir + "/" + id + ".hdt";

            Set<TripleString> tripleSet = new HashSet<>();
            Set<String> subjSet = csMap.get(cs);
            System.out.println("        Found " + subjSet.size() + " subjects.");

            //System.out.println("Containing Obama: " + subjSet.contains("http://dbpedia.org/resource/Barack_Obama"));
            boolean obama = false;


            int subjNum = 1;
            for (String subj : subjSet) {

                if (subj.equals("http://data.nytimes.com/47452218948077706853")) obama = true;

                if (subjNum % 1000 == 0) System.out.print("\r        Subject " + subjNum);
                for (Tuple<String, String> tpl : subjMap.get(subj)) {
                    if (!cs.hasPredicate(tpl.x)) continue;
                    HDT hdt = hdtMap.get(tpl.y);
                    IteratorTripleString its1 = hdt.search(subj, tpl.x, "");
                    while (its1.hasNext()) {
                        tripleSet.add(its1.next());
                    }
                }

                if ((subjNum % 10000000) == 0) {
                    System.out.println("Generating intermediate...");

                    String id1 = gen.nextString();

                    if (obama) {
                        System.out.println("Obama: " + id1 + " " + cs.toString());
                        obama = false;
                    }

                    String outpath1 = oDir + "/" + id1 + ".hdt";

                    System.out.print("\n");
                    System.out.println("        Saving HDT as " + outpath1);

                    HDT newHdt;
                    try {
                        newHdt = HDTManager.generateHDT(new MergedHDTIterator<>(tripleSet), "http://colchain.org/fragments#" + id1, new HDTSpecification(), null);
                    } catch (Exception e) {
                        continue;
                    }

                    newHdt.saveToHDT(outpath1, null);
                    System.out.println("        Saved HDT file: " + outpath1);
                    FileWriter writer = new FileWriter(oDir + "/" + id1 + ".chs");
                    writer.write(getCharSetString(cs));
                    writer.close();

                    tripleSet = new HashSet<>();
                }

                subjNum++;
            }

            if (obama) {
                System.out.println("Obama: " + id + " " + cs.toString());
                obama = false;
            }

            System.out.print("\n");
            System.out.println("        Saving HDT as " + outpath);

            HDT newHdt;
            try {
                newHdt = HDTManager.generateHDT(new MergedHDTIterator<>(tripleSet), "http://colchain.org/fragments#" + id, new HDTSpecification(), null);
            } catch (Exception e) {
                continue;
            }

            newHdt.saveToHDT(outpath, null);

            System.out.println("        Saved HDT file: " + outpath);
            FileWriter writer = new FileWriter(oDir + "/" + id + ".chs");
            writer.write(getCharSetString(cs));
            writer.close();

            System.out.println("        Saved CS file");
        }
    }

    private void createFragmentsFromFile(String file, String oDir, String cDir) throws IOException, NotFoundException {
        int threshold = 86;
        Map<String, Set<String>> subjMap = new HashMap<>();
        RandomString gen = new RandomString(10);

        Map<String, Set<org.colchain.index.util.Tuple<String, String>>> communityMap = new HashMap<>();
        List<String> communities = new ArrayList<>();

        File[] cDirs = new File(cDir).listFiles();
        System.out.println("Creating communities...");
        for(File c : cDirs) {
            String id = c.getName();
            communities.add(id);
            communityMap.put(id, new HashSet<>());

            String dir = oDir + (oDir.endsWith("/")? "" : "/") + id;
            new File(dir).mkdirs();
        }

        System.out.println("Finding characteristic sets..");
        HDT hdt = HDTManager.mapIndexedHDT(file);

        IteratorTripleString iterator = hdt.search("", "", "");
        while (iterator.hasNext()) {
            TripleString triple = iterator.next();
            String subj = triple.getSubject().toString();
            if (subjMap.containsKey(subj)) subjMap.get(subj).add(triple.getPredicate().toString());
            else {
                Set<String> set = new HashSet<>();
                set.add(triple.getPredicate().toString());
                subjMap.put(subj, set);
            }
        }

        System.out.println("Found " + subjMap.size() + " subjects.");
        System.out.println("Finding characteristic sets...");
        Set<ICharacteristicSet> csSet = new HashSet<>();
        Map<ICharacteristicSet, Set<String>> csMap = new HashMap<>();

        for (String subj : subjMap.keySet()) {
            ICharacteristicSet csSet1 = CharacteristicSetFactory.create(subjMap.get(subj));
            csSet.add(csSet1);

            if (csMap.containsKey(csSet1)) csMap.get(csSet1).add(subj);
            else {
                Set<String> ssset = new HashSet<>();
                ssset.add(subj);
                csMap.put(csSet1, ssset);
            }
        }

        System.out.println("Found " + csSet.size() + " characteristic sets.");
        System.out.println("Pruning subsets...");
        Set<ICharacteristicSet> cSets = new HashSet<>(csSet);
        int pruned = 0;

        for (ICharacteristicSet cSet : cSets) {
            if (!csMap.containsKey(cSet)) continue;

            ICharacteristicSet superCs = getSuperset(cSet, csMap.keySet());
            if (superCs == null) continue;

            csMap.get(superCs).addAll(csMap.get(cSet));
            csMap.remove(cSet);
            csSet.remove(cSet);

            pruned++;
            System.out.print("\rPruned " + pruned + " characteristic sets.");

            if(csSet.size() <= threshold) break;
        }

        System.out.print("\n");
        System.out.println("Pruning infrequent characteristic sets...");
        pruned = 0;
        Set<ICharacteristicSet> safe = new HashSet<>();

        while(csSet.size() > threshold) {
            ICharacteristicSet cSet = getSmallestCs(csMap, safe);
            Set<String> remainingPreds = new HashSet<>(cSet.getPredicates());
            while (remainingPreds.size() > 0) {
                ICharacteristicSet largest = getLargest(remainingPreds, csSet);
                if (largest == null) {
                    Set<String> preds = new HashSet<>(cSet.getPredicates());
                    preds.retainAll(remainingPreds);
                    ICharacteristicSet cs = CharacteristicSetFactory.create(preds);

                    safe.add(cs);
                    if(cs.equals(cSet)) break;
                    csMap.put(cs, csMap.get(cSet));
                    csSet.add(cs);
                    break;
                }

                csMap.get(largest).addAll(csMap.get(cSet));
                remainingPreds.removeAll(largest.getPredicates());
            }

            if(!safe.contains(cSet)) {
                csMap.remove(cSet);
                csSet.remove(cSet);
            }

            pruned++;
            System.out.print("\rPruned " + pruned + " characteristic sets.");
        }

        System.out.print("\n");
        System.out.println("Left with " + csSet.size() + " characteristic sets.");
        Random rand = new Random();

        for (ICharacteristicSet cs : csSet) {
            System.out.println("    Handling CS " + cs.toString());

            String id = gen.nextString();
            String cid = communities.get(rand.nextInt(communities.size()));
            String outpath = oDir + "/" + cid + "/" + id + ".hdt";

            Set<TripleString> tripleSet = new HashSet<>();
            Set<String> subjSet = csMap.get(cs);
            System.out.println("        Found " + subjSet.size() + " subjects.");
            int subjNum = 1;
            for (String subj : subjSet) {
                if (subjNum % 1000 == 0) System.out.print("\r        Subject " + subjNum);
                for (String pred : subjMap.get(subj)) {
                    if (!cs.hasPredicate(pred)) continue;
                    IteratorTripleString its1 = hdt.search(subj, pred, "");
                    while (its1.hasNext()) {
                        tripleSet.add(its1.next());
                    }
                }

                /*if ((subjNum % 10000000) == 0) {
                    System.out.println("Generating intermediate...");

                    String id1 = gen.nextString();
                    String outpath1 = oDir + "/" + id1 + ".hdt";

                    System.out.print("\n");
                    System.out.println("        Saving HDT as " + outpath1);

                    HDT newHdt;
                    try {
                        newHdt = HDTManager.generateHDT(new MergedHDTIterator<>(tripleSet), "http://colchain.org/fragments#" + id1, new HDTSpecification(), null);
                    } catch (Exception e) {
                        continue;
                    }

                    newHdt.saveToHDT(outpath1, null);
                    System.out.println("        Saved HDT file: " + outpath1);
                    FileWriter writer = new FileWriter(oDir + "/" + id1 + ".chs");
                    writer.write(getCharSetString(cs));
                    writer.close();

                    tripleSet = new HashSet<>();
                }*/

                subjNum++;
            }

            System.out.print("\n");
            System.out.println("        Saving HDT as " + outpath);

            HDT newHdt;
            try {
                newHdt = HDTManager.generateHDT(new MergedHDTIterator<>(tripleSet), "http://colchain.org/fragments#" + id, new HDTSpecification(), null);
            } catch (Exception e) {
                continue;
            }

            newHdt.saveToHDT(outpath, null);

            System.out.println("        Saved HDT file: " + outpath);
            FileWriter writer = new FileWriter(oDir + "/" + cid + "/" + id + ".chs");
            writer.write(getCharSetString(cs));
            writer.close();

            System.out.println("        Saved CS file");
        }
    }

    private ICharacteristicSet getSmallestCs(Map<ICharacteristicSet, Set<String>> csMap, Set<ICharacteristicSet> safe) {
        ICharacteristicSet cs = null;
        int small = Integer.MAX_VALUE;

        for(ICharacteristicSet css : csMap.keySet()) {
            if(safe.contains(css)) continue;
            if(csMap.get(css).size() < small) {
                cs = css;
                small = csMap.get(css).size();
            }
        }

        return cs;
    }

    private ICharacteristicSet getLargest(Set<String> preds, Set<ICharacteristicSet> css) {
        ICharacteristicSet largest = null;
        int largestCount = 0;
        ICharacteristicSet set = CharacteristicSetFactory.create(preds);

        for (ICharacteristicSet cs : css) {
            if (cs.equals(set)) continue;

            int count = 0;
            for (String pred : preds) {
                if (cs.hasPredicate(pred)) count++;
            }
            if (count > largestCount) {
                largest = cs;
                largestCount = count;
            }
        }
        return largest;
    }

    private Set<ICharacteristicSet> readCharacteristicSets(String dirname) throws IOException {
        File[] files = new File(dirname).listFiles();
        Set<ICharacteristicSet> set = new HashSet<>();

        for (File file : files) {
            if (!file.getName().contains(".chs")) continue;

            Set<String> preds = new HashSet<>();
            Scanner reader = new Scanner(file);
            while (reader.hasNextLine()) {
                String pred = reader.nextLine();
                if (pred.equals("")) continue;
                preds.add(pred);
            }
            set.add(CharacteristicSetFactory.create(preds));
        }
        return set;
    }

    private ICharacteristicSet getSuperset(ICharacteristicSet cs, Set<ICharacteristicSet> csSet) {
        for (ICharacteristicSet css : csSet) {
            if (cs.equals(css)) continue;
            if (css.hasPredicates(new HashSet<>(cs.getPredicates()))) return css;
        }

        return null;
    }

    private boolean isSameCS(ICharacteristicSet cs, Set<Tuple<String, String>> tplSet) {
        Set<String> preds = new HashSet<>();
        for (Tuple<String, String> tpl : tplSet) {
            preds.add(tpl.x);
        }

        return cs.equals(CharacteristicSetFactory.create(preds));
    }

    private String getCharSetString(ICharacteristicSet set) {
        StringBuilder sb = new StringBuilder();
        boolean hasFirst = false;

        for (String pred : set.getPredicates()) {
            if (hasFirst) sb.append("\n");
            else hasFirst = true;
            sb.append(pred);
        }

        return sb.toString();
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

        new File("setup/hdt").mkdirs();
        new File("setup/updates").mkdirs();

        // Create community distribution
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


            // Create updates to fragments
            /*BufferedReader reader = new BufferedReader(new FileReader(fDir.getAbsolutePath() + "/fragments"));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.equals("")) continue;
                String[] ws = line.split(";");
                System.out.println(ws[0]);

                for (int i = 1; i <= 100; i = i * 10) {
                    ChainEntry entry = ChainEntry.getInitialEntry();
                    Set<Triple> added = new HashSet<>();

                    for (int k = 1; k <= i; k++) {
                        List<Operation> operations = getOperations(ws[0], added);
                        for (Operation op : operations) {
                            if (op.getType() == Operation.OperationType.ADD)
                                added.add(op.getTriple());
                            else
                                added.remove(op.getTriple());
                        }

                        ITransaction transaction = TransactionFactory.getTransaction(operations, ws[1], ids.get(rand.nextInt(ids.size())), rs.nextString(), k);
                        entry = new ChainEntry(transaction, entry);
                    }

                    String dname = "setup/updates/" + ws[1];
                    new File(dname).mkdirs();
                    FileWriter fout = new FileWriter(dname + "/" + i);
                    fout.write(gson.toJson(entry));
                    fout.close();
                }
            }*/
        }

        out.write(gson.toJson(map));
        out.flush();
        out.close();

        // Create keys
        File prDir = new File("setup/keys/private/");
        prDir.mkdirs();
        File puDir = new File("setup/keys/public/");
        puDir.mkdirs();

        for (int i = 0; i < nodes; i++) {
            KeyPair kp = CryptoUtils.generateKeyPair();

            byte[] priv = kp.getPrivate().getEncoded();
            byte[] pub = kp.getPublic().getEncoded();

            OutputStream os = new FileOutputStream("setup/keys/private/" + i);
            os.write(priv);
            os.close();

            os = new FileOutputStream("setup/keys/public/" + i);
            os.write(pub);
            os.close();
        }
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

        if (node < nodes) {
            PrivateKey pKey = CryptoUtils.getPrivateKey(FileUtils.readFileToByteArray(new File(setup + "keys/private/" + node)));
            PublicKey puKey = CryptoUtils.getPublicKey(FileUtils.readFileToByteArray(new File(setup + "keys/public/" + node)));
            KeyPair keys = new KeyPair(puKey, pKey);
            AbstractNode.getState().setKeyPair(keys);
        }

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
            System.out.println("Handling community " + s);
            Tuple<Integer, Set<Integer>> tpl = map.get(s);
            Set<Integer> set = tpl.y;
            int owner = tpl.x;
            byte[] oKey = FileUtils.readFileToByteArray(new File(setup + "keys/public/" + owner));

            Set<CommunityMember> participants = new HashSet<>();
            Set<CommunityMember> observers = new HashSet<>();
            for (int i = 0; i < nodes; i++) {
                if (set.contains(i)) {
                    participants.add(new CommunityMember(ids.get(i), "http://172.21.232.208:3" + String.format("%03d", i)));
                    //participants.add(new CommunityMember(ids.get(i), "http://localhost:8080/colchain-0.1"));
                } else {
                    observers.add(new CommunityMember(ids.get(i), "http://172.21.232.208:3" + String.format("%03d", i)));
                    //observers.add(new CommunityMember(ids.get(i), "http://172.21.232.208:808" + i + "/kc"));
                }
            }
            boolean participant;
            Community.MemberType mt;
            if (set.contains(node)) {
                participant = true;
                mt = Community.MemberType.PARTICIPANT;
            } else {
                participant = false;
                mt = Community.MemberType.OBSERVER;
            }

            Community c = new Community(s, "Community " + cnum, mt, participants, observers);
            cnum++;
            AbstractNode.getState().addCommunity(c);

            File[] fFiles = new File(dirname + (dirname.endsWith("/") ? "" : "/") + s).listFiles();
            for (File fFile : fFiles) {
                if (fFile.getName().contains(".index") || fFile.getName().contains(".chs")) continue;
                System.out.println("    Handling fragment " + fFile.getName());
                String path = dirname + (dirname.endsWith("/") ? "" : "/") + s + "/" + fFile.getName();

                String predFileName = fFile.getAbsolutePath().replace(".hdt", ".chs");
                ICharacteristicSet cs = getCSFromFilename(predFileName);
                String fid = fFile.getName().replace(".hdt", "");
                if (participant) {
                    AbstractNode.getState().addNewFragment(fid, fid, path, s, oKey, cs);
                } else {
                    AbstractNode.getState().addNewObservedFragment(fid, fid, s, oKey, cs);
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

    private void handleStart(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String config = request.getParameter("config");
        initConfig(config);
        WebInterfaceServlet.INIT = true;
        String setup = request.getParameter("setup");
        String dirname = request.getParameter("dir");
        int node = Integer.parseInt(request.getParameter("id"));
        int nodes = Integer.parseInt(request.getParameter("nodes"));
        int chain;
        String ch = request.getParameter("chain");
        if (ch == null || ch.equals(""))
            chain = 0;
        else
            chain = Integer.parseInt(request.getParameter("chain"));
        AbstractNode.getState().setAddress("http://172.21.232.208:3" + String.format("%03d", node));
        setup = setup + (setup.endsWith("/") ? "" : "/");

        if (node < nodes) {
            PrivateKey pKey = CryptoUtils.getPrivateKey(FileUtils.readFileToByteArray(new File(setup + "keys/private/" + node)));
            PublicKey puKey = CryptoUtils.getPublicKey(FileUtils.readFileToByteArray(new File(setup + "keys/public/" + node)));
            KeyPair keys = new KeyPair(puKey, pKey);
            AbstractNode.getState().setKeyPair(keys);
        }

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
            Tuple<Integer, Set<Integer>> tpl = map.get(s);
            Set<Integer> set = tpl.y;
            int owner = tpl.x;
            byte[] oKey = FileUtils.readFileToByteArray(new File(setup + "keys/public/" + owner));

            Set<CommunityMember> participants = new HashSet<>();
            Set<CommunityMember> observers = new HashSet<>();
            for (int i = 0; i < nodes; i++) {
                if (set.contains(i)) {
                    participants.add(new CommunityMember(ids.get(i), "http://172.21.232.208:3" + String.format("%03d", i)));
                    //participants.add(new CommunityMember(ids.get(i), "http://172.21.232.208:808" + i + "/kc"));
                } else {
                    observers.add(new CommunityMember(ids.get(i), "http://172.21.232.208:3" + String.format("%03d", i)));
                    //observers.add(new CommunityMember(ids.get(i), "http://172.21.232.208:808" + i + "/kc"));
                }
            }
            boolean participant;
            Community.MemberType mt;
            if (set.contains(node)) {
                participant = true;
                mt = Community.MemberType.PARTICIPANT;
            } else {
                participant = false;
                mt = Community.MemberType.OBSERVER;
            }

            Community c = new Community(s, "Community " + cnum, mt, participants, observers);
            cnum++;
            AbstractNode.getState().addCommunity(c);

            String fFile = dirname + (dirname.endsWith("/") ? "" : "/") + s + "/fragments";
            BufferedReader reader = new BufferedReader(new FileReader(fFile));
            String line = reader.readLine();
            while (line != null) {
                if (line.equals("")) {
                    line = reader.readLine();
                    continue;
                }
                String[] ws = line.split(";");
                String path = fFile.replace("/fragments", "/" + ws[1] + ".hdt");

                if (participant) {
                    if (chain > 0) {
                        String filename = setup + "updates/" + ws[1] + "/" + chain;
                        String j = FileUtils.readFileToString(new File(filename), StandardCharsets.UTF_8);

                        Gson g = new GsonBuilder().registerTypeAdapter(ChainEntry.class, new ChainSerializer()).create();
                        ChainEntry entry = g.fromJson(j, ChainEntry.class);

                        AbstractNode.getState().addNewFragment(ws[1], ws[0], path, s, oKey, entry, CharacteristicSetFactory.create());
                    } else {
                        AbstractNode.getState().addNewFragment(ws[1], ws[0], path, s, oKey, CharacteristicSetFactory.create());
                    }
                } else {
                    AbstractNode.getState().addNewObservedFragment(ws[1], ws[0], s, oKey, CharacteristicSetFactory.create());
                }

                line = reader.readLine();
            }
            reader.close();
        }
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
        private final ResultSet rs;

        public ExecutorCallable(String qStr) {
            Query query = QueryFactory.create(qStr);
            final LothbrokGraph graph = new LothbrokGraph();
            Model model = ModelFactory.createModelForGraph(graph);
            qExecutor = QueryExecutionFactory.create(query, model);
            rs = qExecutor.execSelect();
        }

        public ExecutorCallable(String qStr, long timestamp) {
            Query query = QueryFactory.create(qStr);
            final LothbrokGraph graph = new LothbrokGraph(timestamp);
            Model model = ModelFactory.createModelForGraph(graph);
            qExecutor = QueryExecutionFactory.create(query, model);
            rs = qExecutor.execSelect();
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
