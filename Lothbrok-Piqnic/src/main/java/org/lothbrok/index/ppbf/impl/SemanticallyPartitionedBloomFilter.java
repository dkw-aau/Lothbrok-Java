package org.lothbrok.index.ppbf.impl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.lothbrok.index.graph.IGraph;
import org.lothbrok.index.ppbf.IBloomFilter;
import org.lothbrok.index.ppbf.IPartitionedBloomFilter;
import org.lothbrok.stars.StarString;
import org.lothbrok.utils.RandomString;
import org.lothbrok.utils.Triple;
import org.lothbrok.utils.Tuple;
import org.rdfhdt.hdt.exceptions.NotImplementedException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class SemanticallyPartitionedBloomFilter implements IPartitionedBloomFilter<String> {
    private final String dirname;
    private RandomString rs = new RandomString();
    private final long numElements;
    private final double maxFpp;
    private long numInsertedElements = 0;

    private final IBloomFilter<String> subjectPartition;
    private final IBloomFilter<String> predicatePartition;
    private Map<String, IBloomFilter<String>> objectMap = new HashMap<>();

    public SemanticallyPartitionedBloomFilter(long numElements, double maxFpp, String dir) {
        this.dirname = dir;
        this.numElements = numElements;
        this.maxFpp = maxFpp;

        File d = new File(this.dirname);
        File predFile = new File(this.dirname + "/" + "predicates");

        if (!d.exists() || !d.isDirectory() || !predFile.exists()) {
            if (d.exists()) d.delete();
            System.out.println("Building SPBF for " + dir);
            d.mkdirs();
            subjectPartition = PrefixPartitionedBloomFilter.create(numElements, maxFpp, dir + "/subjects.ppbf");
            predicatePartition = PrefixPartitionedBloomFilter.create(numElements, maxFpp, dir + "/predicates.ppbf");
            try {
                predFile.createNewFile();
            } catch (IOException e) {
            }
        } else {
            subjectPartition = PrefixPartitionedBloomFilter.create(numElements, maxFpp, dir + "/subjects.ppbf");
            predicatePartition = PrefixPartitionedBloomFilter.create(numElements, maxFpp, dir + "/predicates.ppbf");
            try {
                FileReader fr = new FileReader(predFile);
                BufferedReader br = new BufferedReader(fr);
                String line;
                while ((line = br.readLine()) != null) {
                    if (!line.contains(";")) continue;
                    String[] ws = line.split(";");

                    if(ws[0].equals("subject")) {
                        subjectPartition.setNumInsertedElements(Long.parseLong(ws[1]));
                        continue;
                    }

                    if(ws[0].equals("predicate")) {
                        predicatePartition.setNumInsertedElements(Long.parseLong(ws[1]));
                        continue;
                    }

                    objectMap.put(ws[0], PrefixPartitionedBloomFilter.create(numElements, maxFpp, dir + "/" + ws[1] + ".ppbf"));
                    objectMap.get(ws[0]).setNumInsertedElements(Long.parseLong(ws[2]));
                }
                fr.close();
            } catch (IOException e) {
            }
            numInsertedElements += subjectPartition.elementCount() + predicatePartition.elementCount();
            for (String pred : objectMap.keySet()) {
                numInsertedElements += objectMap.get(pred).elementCount();
            }
        }
    }

    public static boolean exists(String dirname) {
        System.out.println("Looking for dir " + dirname);
        if(!new File(dirname).exists()) return false;
        System.out.println("Dir exists.");
        String dname = dirname + (dirname.endsWith("/")? "" : "/");
        System.out.println("Dname: " + dname);
        File predfile = new File(dname + "predicates");

        boolean contains = true;
        if(!predfile.exists()) contains =  false;
        if(!new File(dname + "/subjects.ppbf").exists() || !new File(dname + "/predicates.ppbf").exists()) contains = false;

        try {
            Scanner scanner = new Scanner(predfile);
            while(scanner.hasNextLine()) {
                String pred = scanner.nextLine();
                String[] ws = pred.split(";");

                if(!new File(dname + ws[1] + ".ppbf").exists()) contains = false;
            }
        } catch (FileNotFoundException e) {
            contains = false;
        }

        if(!contains) new File(dirname).delete();
        return contains;
    }

    @Override
    public void setNumInsertedElements(long count) {
        this.numInsertedElements = count;
    }

    private SemanticallyPartitionedBloomFilter() {
        this.dirname = "";
        subjectPartition = null;
        predicatePartition = null;
        this.maxFpp = 0;
        this.numElements = 0;
        this.numInsertedElements = 0;
    }

    public static SemanticallyPartitionedBloomFilter create(long numElements, double maxFpp, String file) {
        return new SemanticallyPartitionedBloomFilter(numElements, maxFpp, file);
    }

    public static SemanticallyPartitionedBloomFilter create(double maxFpp, String file) {
        return new SemanticallyPartitionedBloomFilter(500000, maxFpp, file);
    }

    public static SemanticallyPartitionedBloomFilter create(String file) {
        return new SemanticallyPartitionedBloomFilter(500000, 0.1, file);
    }

    @Override
    public boolean mightContainSubject(String element) {
        return subjectPartition.mightContain(element);
    }

    @Override
    public boolean mightContainPredicate(String element) {
        return objectMap.containsKey(element);
    }

    @Override
    public boolean mightContainObject(String element, String predicate) {
        return objectMap.containsKey(predicate) && objectMap.get(predicate).mightContain(element);
    }

    @Override
    public IBloomFilter<String> getSubjectPartition() {
        return subjectPartition;
    }

    @Override
    public IBloomFilter<String> getPredicatePartition(String predicate) {
        return objectMap.get(predicate);
    }

    @Override
    public long getTotalCardinality() {
        long cnt = 0;

        for(String pred : objectMap.keySet()) {
            cnt += objectMap.get(pred).estimatedCardinality();
        }

        return cnt;
    }

    @Override
    public void putSubject(String element) {
        //if(subjectPartition.mightContain(element)) return;
        subjectPartition.put(element);
        numInsertedElements++;
    }

    @Override
    public void putPredicate(String element) {
        if (!objectMap.containsKey(element)) {
            predicatePartition.put(element);
            String id = rs.nextString();
            objectMap.put(element, PrefixPartitionedBloomFilter.create(numElements, maxFpp, dirname + "/" + id + ".ppbf"));

            try {
                Files.write(Paths.get(dirname + "/predicates"), (element + ";" + id + ".ppbf" + ";0").getBytes(), StandardOpenOption.APPEND);
            } catch (IOException e) {
            }

            numInsertedElements++;
        }
    }

    @Override
    public void writePredFile() {
        try {
            FileWriter writer = new FileWriter(dirname + "/predicates");
            for (Map.Entry<String, IBloomFilter<String>> entry : objectMap.entrySet()) {
                writer.write(entry.getKey() + ";" + entry.getValue().getFileName().substring(entry.getValue().getFileName().lastIndexOf("/")+1) + ";" + entry.getValue().elementCount() + "\n");
            }
            writer.write("subject;" + subjectPartition.elementCount() + "\n");
            writer.write("predicate;" + predicatePartition.elementCount());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void putObject(String element, String predicate) {
        if (objectMap.containsKey(predicate)) {
            //if(objectMap.get(predicate).mightContain(element)) return;
            objectMap.get(predicate).put(element);
            numInsertedElements++;
        }
    }

    @Override
    public List<IBloomFilter<String>> getPartitions(StarString star, String var) {
        List<IBloomFilter<String>> partitions = new ArrayList<>();
        if (var.equals(star.getSubject().toString())) partitions.add(subjectPartition);
        for (Tuple<CharSequence, CharSequence> tpl : star.getTriples()) {
            if (tpl.x.toString().equals(var)) {
                partitions.add(predicatePartition);
                if (tpl.y.equals(var)) partitions.addAll(objectMap.values());
            } else if (objectMap.containsKey(tpl.x.toString()) && tpl.y.toString().equals(var))
                partitions.add(objectMap.get(tpl.x.toString()));
        }

        return partitions;
    }

    @Override
    public IBloomFilter<String> getPartition(Triple triple, String var) {
        if (var.equals(triple.getSubject())) return subjectPartition;
        if (var.equals(triple.getPredicate())) return predicatePartition;
        if (var.equals(triple.getObject()) && objectMap.containsKey(triple.getPredicate()))
            return objectMap.get(triple.getPredicate());
        return null;
    }

    @Override
    public IBloomFilter<String> getPartition(StarString star, String var) {
        if (var.equals(star.getSubject().toString())) return subjectPartition;
        for (Tuple<CharSequence, CharSequence> tpl : star.getTriples()) {
            if (tpl.x.toString().equals(var)) {
                return predicatePartition;
            } else if (objectMap.containsKey(tpl.x.toString()) && tpl.y.toString().equals(var))
                return objectMap.get(tpl.x.toString());
        }

        return null;
    }

    @Override
    public boolean mightContainConstants(StarString star) {
        if (!star.getSubject().toString().startsWith("?")) {
            if (!subjectPartition.mightContain(star.getSubject().toString())) return false;
        }
        for (Tuple<CharSequence, CharSequence> tpl : star.getTriples()) {
            String pred = tpl.x.toString(), obj = tpl.y.toString();
            if (!pred.startsWith("?") && !this.objectMap.containsKey(pred)) return false;
            if (!obj.startsWith("?") && !this.objectMap.get(pred).mightContain(obj)) return false;
        }
        return true;
    }

    @Override
    public boolean hasEmptyPartition() {
        if(subjectPartition.elementCount() == 0) return true;
        if(predicatePartition.elementCount() == 0) return true;
        for(IBloomFilter<String> part : this.objectMap.values()) {
            if(part.elementCount() == 0) return true;
        }
        return false;
    }

    @Override
    public void resetPartitionSizes() {
        subjectPartition.setNumInsertedElements(0);
        predicatePartition.setNumInsertedElements(0);
        objectMap.clear();
        objectMap = new HashMap<>();
    }

    @Override
    public void deleteFiles() {
        subjectPartition.deleteFile();
        predicatePartition.deleteFile();
        new File(dirname + "/predicates").delete();
        for (String pred : objectMap.keySet()) {
            objectMap.get(pred).deleteFile();
        }
        new File(dirname).delete();
    }

    @Override
    public long elementCount() {
        return numInsertedElements;
    }

    @Override
    public boolean mightContain(String element) {
        if (subjectPartition.mightContain(element) || objectMap.containsKey(element)) return true;
        for (String pred : objectMap.keySet()) {
            if (objectMap.get(pred).mightContain(element)) return true;
        }
        return false;
    }

    @Override
    public void put(String element) {
        throw new NotImplementedException("Method invalid for partitioned bloom filter.");
    }

    @Override
    public IBloomFilter<String> intersect(IBloomFilter<String> other, String filename) {
        throw new NotImplementedException("Method invalid for partitioned bloom filter.");
    }

    @Override
    public String getFileName() {
        return dirname;
    }

    @Override
    public IBloomFilter<String> copy() {
        return new SemanticallyPartitionedBloomFilter(numElements, maxFpp, dirname);
    }

    @Override
    public void clear() {
        throw new NotImplementedException("Method invalid for partitioned bloom filter.");
    }

    @Override
    public boolean isEmpty() {
        if (!subjectPartition.isEmpty() || !predicatePartition.isEmpty()) return false;
        for (String pred : objectMap.keySet()) {
            if (!objectMap.get(pred).isEmpty()) return false;
        }
        return true;
    }

    @Override
    public boolean hasPredicates(Collection<String> predicates) {
        for (String predicate : predicates) {
            if (!this.objectMap.containsKey(predicate)) return false;
        }
        return true;
    }

    @Override
    public void deleteFile() {
        deleteFiles();
    }

    @Override
    public long estimatedCardinality() {
        return numInsertedElements;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SemanticallyPartitionedBloomFilter that = (SemanticallyPartitionedBloomFilter) o;
        return dirname.equals(that.dirname) && subjectPartition.equals(that.subjectPartition) && predicatePartition.equals(that.predicatePartition) && objectMap.equals(that.objectMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dirname, subjectPartition, predicatePartition, objectMap);
    }

    @Override
    public String toString() {
        Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
        return gson.toJson(this);
    }

    public static SemanticallyPartitionedBloomFilter fromString(String str) {
        Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
        return gson.fromJson(str, SemanticallyPartitionedBloomFilter.class);
    }
}
