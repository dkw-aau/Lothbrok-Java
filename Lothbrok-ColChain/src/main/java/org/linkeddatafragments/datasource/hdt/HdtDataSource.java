package org.linkeddatafragments.datasource.hdt;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.transaction.ITransaction;
import org.apache.commons.io.FileUtils;
import org.linkeddatafragments.datasource.DataSourceBase;
import org.linkeddatafragments.datasource.DataSourceFactory;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.fragments.IFragmentRequestParser;
import org.linkeddatafragments.fragments.spf.SPFRequestParserForJenaBackends;
import org.linkeddatafragments.fragments.tpf.BRTPFRequestParserForJenaBackends;
import org.linkeddatafragments.fragments.tpf.TPFRequestParserForJenaBackends;
import org.lothbrok.index.ppbf.IBloomFilter;
import org.lothbrok.index.ppbf.IPartitionedBloomFilter;
import org.lothbrok.index.ppbf.impl.PrefixPartitionedBloomFilter;
import org.lothbrok.index.ppbf.impl.SemanticallyPartitionedBloomFilter;
import org.lothbrok.utils.Triple;
import org.lothbrok.utils.Tuple;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorStarString;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.LRUCache;
import org.rdfhdt.hdtjena.NodeDictionary;


public class HdtDataSource extends DataSourceBase {

    /**
     * HDT Datasource
     */
    protected HDT datasource;

    private final String hdtFile;

    /**
     * The dictionary
     */
    protected NodeDictionary dictionary;

    protected LRUCache<Tuple<Long, Long>, IteratorStarString> pageCache;

    /**
     * Creates a new HdtDataSource.
     *
     * @param title       title of the datasource
     * @param description datasource description
     * @param hdtFile     the HDT datafile
     * @throws IOException if the file cannot be loaded
     */
    public HdtDataSource(String title, String description, String hdtFile) throws IOException {
        super(title, description);

        System.out.println("        Indexing HDT...");
        datasource = HDTManager.mapIndexedHDT(hdtFile, null);
        dictionary = new NodeDictionary(datasource.getDictionary());
        this.hdtFile = hdtFile;

        this.pageCache = new LRUCache<>(500);
    }

    public String getFile() {
        return hdtFile;
    }

    private static String readFile(String filePath) {
        String content = "";
        try {
            content = new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content;
    }

    @Override
    public HDT getHdt() {
        return datasource;
    }

    @Override
    public void updateHdt(ITransaction transaction) {
        IteratorTripleString it;
        try {
            it = datasource.search("", "", "");
        } catch (NotFoundException e) {
            return;
        }

        try {
            HDT newHdt = HDTManager.generateHDT(new IteratorUpdateString(it, transaction.getAdditions(), transaction),
                    "http://colchain.org/fragments#" + transaction.getFragmentId(), new HDTSpecification(), null);
            datasource.close();
            File f = new File(hdtFile);
            f.delete();
            f = new File(hdtFile + ".index.v1-1");
            f.delete();
            newHdt.saveToHDT(hdtFile, null);
            datasource = HDTManager.mapIndexedHDT(hdtFile, null);
            dictionary = new NodeDictionary(datasource.getDictionary());
        } catch (ParserException | IOException e) {
        }
    }

    @Override
    public IFragmentRequestParser getRequestParser(IDataSource.ProcessorType processor) {
        if (processor == ProcessorType.TPF)
            return TPFRequestParserForJenaBackends.getInstance();
        else if (processor == ProcessorType.BRTPF)
            return BRTPFRequestParserForJenaBackends.getInstance();
        return SPFRequestParserForJenaBackends.getInstance();
    }

    @Override
    public int numTriples() {
        return (int) datasource.getTriples().getNumberOfElements();
    }

    @Override
    public int numSubjects() {
        return datasource.getDictionary().getSubjects().getNumberOfElements();
    }

    @Override
    public int numPredicates() {
        return datasource.getDictionary().getPredicates().getNumberOfElements();
    }

    @Override
    public int numObjects() {
        return datasource.getDictionary().getObjects().getNumberOfElements();
    }

    @Override
    public IFragmentRequestProcessor getRequestProcessor(IDataSource.ProcessorType processor) {
        if (processor == ProcessorType.TPF)
            return new HdtBasedRequestProcessorForTPFs(datasource, dictionary);
        else if(processor == ProcessorType.BRTPF)
            return new HdtBasedRequestProcessorForBRTPFs(datasource, dictionary);
        return new HdtBasedRequestProcessorForSPFs(datasource, dictionary, pageCache);
    }

    @Override
    public void deleteBloomFilter() {
        String filename = AbstractNode.getState().getDatastore() + "index/" + title + ".hdt.ppbf";
        File file = new File(filename);
        File file1 = new File(filename + "p");

        if (file.exists()) file.delete();
        if (file1.exists()) file1.delete();
    }

    @Override
    public void reload() {
        try {
            datasource = HDTManager.mapIndexedHDT(hdtFile, null);
        } catch (IOException e) {
        }
    }

    @Override
    public IBloomFilter<String> createBloomFilter() {
        String filename = AbstractNode.getState().getDatastore() + "index/" + title + ".hdt.ppbf";
        File f = new File(filename);
        if (f.exists()) return PrefixPartitionedBloomFilter.create(filename);
        IBloomFilter<String> filter = PrefixPartitionedBloomFilter.create(filename);

        try {
            Iterator<? extends CharSequence> it = datasource.getDictionary().getSubjects().getSortedEntries();
            while (it.hasNext()) {
                filter.put(it.next().toString());
            }
        } catch (NullPointerException e) {
        }

        try {
            Iterator<? extends CharSequence> it = datasource.getDictionary().getPredicates().getSortedEntries();
            while (it.hasNext()) {
                filter.put(it.next().toString());
            }
        } catch (NullPointerException e) {
        }

        try {
            Iterator<? extends CharSequence> it = datasource.getDictionary().getObjects().getSortedEntries();
            while (it.hasNext()) {
                filter.put(it.next().toString());
            }
        } catch (NullPointerException e) {
        }

        return filter;
    }

    @Override
    public IPartitionedBloomFilter<String> createPartitionedBloomFilter() {
        String dirname = AbstractNode.getState().getDatastore() + "index/" + title;
        File d = new File(dirname);
        if (d.exists()) {
            IPartitionedBloomFilter<String> bloom = SemanticallyPartitionedBloomFilter.create(dirname);
            if(!bloom.hasEmptyPartition()) return bloom;
            bloom.resetPartitionSizes();
        }

        IteratorTripleString iterator ;
        try {
            iterator = datasource.search("", "", "");
        } catch (NotFoundException e) {
            return createPartitionedBloomFilterDictionaries();
        }
        IPartitionedBloomFilter<String> filter = SemanticallyPartitionedBloomFilter.create(dirname);

        while(iterator.hasNext()) {
            TripleString triple = iterator.next();
            filter.putSubject(triple.getSubject().toString());
            filter.putPredicate(triple.getPredicate().toString());
            filter.putObject(triple.getObject().toString(), triple.getPredicate().toString());
        }

        filter.writePredFile();

        return filter;
    }

    private IPartitionedBloomFilter<String> createPartitionedBloomFilterDictionaries() {
        String dirname = AbstractNode.getState().getDatastore() + "index/" + title;
        File d = new File(dirname);
        if (d.exists()) return SemanticallyPartitionedBloomFilter.create(dirname);
        IPartitionedBloomFilter<String> filter = SemanticallyPartitionedBloomFilter.create(dirname);

        try {
            Iterator<? extends CharSequence> it = datasource.getDictionary().getSubjects().getSortedEntries();
            while (it.hasNext()) {
                filter.putSubject(it.next().toString());
            }
        } catch (NullPointerException e) {
        }

        try {
            Iterator<? extends CharSequence> it = datasource.getDictionary().getPredicates().getSortedEntries();
            while (it.hasNext()) {
                filter.putPredicate(it.next().toString());
            }
        } catch (NullPointerException e) {
        }

        try {
            Iterator<? extends CharSequence> it = datasource.getDictionary().getObjects().getSortedEntries();
            while (it.hasNext()) {
                String obj = it.next().toString();
                IteratorTripleString it1 = datasource.search("", "", obj);
                while(it1.hasNext()) {
                    String pred = it1.next().getPredicate().toString();
                    filter.putObject(obj, pred);
                }
            }
        } catch (NullPointerException | NotFoundException e) {
            e.printStackTrace();
        }

        filter.writePredFile();

        return filter;
    }

    @Override
    public long size() {
        return datasource.getTriples().getNumberOfElements();
    }

    @Override
    public void copy() {
        try {
            FileUtils.copyFile(new File(hdtFile), new File(hdtFile + ".copy"));
            FileUtils.copyFile(new File(hdtFile + ".index.v1-1"), new File(hdtFile + ".index.v1-1.copy"));

            String index = AbstractNode.getState().getDatastore() + "index/" + title + ".hdt.ppbf";
            FileUtils.copyFile(new File(index), new File(index + ".copy"));
            FileUtils.copyFile(new File(index + "p"), new File(index + "p.copy"));
        } catch (IOException e) {
        }
    }

    @Override
    public void restore() {
        try {
            String hdtName = hdtFile + ".copy";
            File hdtFile = new File(hdtName);
            new File(this.hdtFile).delete();
            FileUtils.copyFile(hdtFile, new File(this.hdtFile));

            String indexName = this.hdtFile + ".index.v1-1.copy";
            File indexFile = new File(indexName);
            new File(this.hdtFile + ".index.v1-1").delete();
            FileUtils.copyFile(indexFile, new File(this.hdtFile + ".index.v1-1"));

            String ppbfName = AbstractNode.getState().getDatastore() + "index/" + title + ".hdt.ppbf";
            File ppbfFile = new File(ppbfName + ".copy");
            new File(ppbfName).delete();
            FileUtils.copyFile(ppbfFile, new File(ppbfName));

            String ppbfpName = AbstractNode.getState().getDatastore() + "index/" + title + ".hdt.ppbfp";
            File ppbfpFile = new File(ppbfpName + ".copy");
            new File(ppbfpName).delete();
            FileUtils.copyFile(ppbfpFile, new File(ppbfpName));

            this.datasource.close();
            this.datasource = HDTManager.mapIndexedHDT(this.hdtFile, null);
        } catch (IOException e) {
        }
    }

    @Override
    public IDataSource materializeVersion(Set<Triple> additions, Set<Triple> deletions, long timestamp) {
        IteratorTripleString it;
        try {
            it = datasource.search("", "", "");
        } catch (NotFoundException e) {
            return this;
        }

        try {
            HDT newHdt = HDTManager.generateHDT(new IteratorMaterializeString(it, new LinkedList<>(deletions), additions),
                    "http://colchain.org/fragments#" + title + "-" + timestamp, new HDTSpecification(), null);
            String path = hdtFile.replace(".hdt", "-" + timestamp + ".hdt");
            File f = new File(path);
            if(f.exists()) f.delete();
            f = new File(path + ".index.v1-1");
            if(f.exists()) f.delete();


            newHdt.saveToHDT(path, null);
            return DataSourceFactory.createLocal(this.title + "-" + timestamp, path);
        } catch (IOException | ParserException e) {
            return this;
        }
    }

    @Override
    public void remove() {
        File f = new File(hdtFile);
        if (f.exists()) f.delete();
        f = new File(hdtFile + ".index");
        if (f.exists()) f.delete();
        f = new File(hdtFile + ".index.v1-1");
        if (f.exists()) f.delete();
    }
}
