package org.colchain.colchain.util;

import org.apache.commons.io.FilenameUtils;
import org.lothbrok.utils.Tuple;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class StructurednessCalculator {
    private Map<String, Tuple<HDT, Set<String>>> fragments = new HashMap<>();

    public StructurednessCalculator(String datastore) {
        System.out.println("Finding fragments in " + datastore);
        File ds = new File(datastore);
        for (File n : ds.listFiles()) {
            for (File f : n.listFiles()) {
                String path = FilenameUtils.getPath(f.getPath());
                String filename = f.getName().replace(".hdt", "").replace(".chs", "").replace(".index", "").replace(".v1-1", "");
                if (fragments.containsKey(filename)) continue;
                String src = path + filename + ".hdt";

                File csFile = new File(path + "/" + filename + ".chs");
                // read csFile line by line and add to a set
                Set<String> cs = new HashSet<>();
                try (Stream<String> stream = Files.lines(Paths.get(csFile.getPath()), StandardCharsets.UTF_8)) {
                    stream.forEach(cs::add);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                try {
                    fragments.put(filename, new Tuple<>(HDTManager.mapIndexedHDT(src), cs));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Found " + fragments.size() + " fragments");
        System.out.println();
    }

    public double getStructurednessValue() {
        try {
            Set<String> types = getRDFTypes();
            System.out.println("Total rdf:types: " + types.size());
            double weightedDenomSum = getTypesWeightedDenomSum(types);
            System.out.println("Total weighted denominator sum: " + weightedDenomSum);
            double structuredness = 0;
            long count = 1;
            for (String type : types) {
                long occurenceSum = 0;
                Set<String> typePredicates = getTypePredicates(type);
                long typeInstancesSize = getTypeInstancesSize(type);
                System.out.println("\n" + count + " : Type: " + type);
                for (String predicate : typePredicates) {
                    long predicateOccurences = getOccurences(predicate, type);
                    occurenceSum = (occurenceSum + predicateOccurences);
                    System.out.println("Predicate: " + predicate + " Occurences: " + predicateOccurences);
                }
                double denom = typePredicates.size() * typeInstancesSize;
                if (typePredicates.size() == 0)
                    denom = 1;
                double coverage = occurenceSum / denom;
                System.out.println("Coverage : " + coverage);
                double weightedCoverage = (typePredicates.size() + typeInstancesSize) / weightedDenomSum;
                System.out.println("Weighted Coverage : " + weightedCoverage);
                structuredness = (structuredness + (coverage * weightedCoverage));
                count++;
            }
            return structuredness;
        } catch (NotFoundException | IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * Get occurences of a predicate within a type
     *
     * @param predicate Predicate
     * @param type      Type
     * @return predicateOccurences Predicate occurence value
     */
    public long getOccurences(String predicate, String type) throws NotFoundException, IOException {
        long predicateOccurences = 0;
        Set<String> subjs1 = new HashSet<>();
        Set<String> subjs = new HashSet<>();
        for (String fragment : fragments.keySet()) {
            //Set<String> cs = fragments.get(fragment).getSecond();
            //if (cs.contains(predicate) && cs.contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
            HDT hdt = fragments.get(fragment).getFirst();
            IteratorTripleString it = hdt.search("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", type);
            while (it.hasNext()) {
                TripleString ts = it.next();
                IteratorTripleString it2 = hdt.search(ts.getSubject().toString(), predicate, "");
                if(it2.hasNext() && !subjs.contains(ts.getSubject().toString())) {
                    predicateOccurences++;
                    subjs.add(ts.getSubject().toString());
                }
                //else if(!subjs1.contains(ts.getSubject().toString()))
                //    subjs.add(ts.getSubject().toString());
            }
            //}
        }
        /*for (String subj : subjs) {
            for (String fragment : fragments.keySet()) {
                Set<String> cs = fragments.get(fragment).getSecond();
                if (cs.contains(predicate)) {
                    HDT hdt = fragments.get(fragment).getFirst();
                    IteratorTripleString it = hdt.search(subj, predicate, "");
                    if (it.hasNext()) {
                        predicateOccurences++;
                        break;
                    }
                }
            }
        }*/

        return predicateOccurences;
    }

    /**
     * Get the denominator of weighted sum all types. Please see Duan et. all paper apple oranges
     *
     * @param types Set of rdf:types
     * @return sum Sum of weighted denominator
     */
    public double getTypesWeightedDenomSum(Set<String> types) throws NotFoundException, IOException {
        double sum = 0;
        for (String type : types) {
            long typeInstancesSize = getTypeInstancesSize(type);
            long typePredicatesSize = getTypePredicates(type).size();
            sum = sum + typeInstancesSize + typePredicatesSize;
        }
        return sum;
    }

    /**
     * Get the number of distinct instances of a specfici type
     *
     * @param type Type or class name
     * @return typeInstancesSize No of instances of type
     */
    private long getTypeInstancesSize(String type) throws IOException, NotFoundException {
        long typeInstancesSize = 0;
        //Set<String> subjs1 = new HashSet<>();
        Set<String> subjs = new HashSet<>();
        for (String fragment : fragments.keySet()) {
            Set<String> cs = fragments.get(fragment).getSecond();
            if (cs.contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
                HDT hdt = fragments.get(fragment).getFirst();
                IteratorTripleString it = hdt.search("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", type);
                while (it.hasNext()) {
                    TripleString ts = it.next();
                    IteratorTripleString it2 = hdt.search(ts.getSubject().toString(), "", "");
                    if(it2.hasNext() && !subjs.contains(ts.getSubject().toString())) {
                        typeInstancesSize++;
                        subjs.add(ts.getSubject().toString());
                    }
                    //else if(!subjs1.contains(ts.getSubject().toString()))
                        //subjs.add(ts.getSubject().toString());
                }
            }
        }
        /*for (String subj : subjs) {
            for (String fragment : fragments.keySet()) {
                HDT hdt = fragments.get(fragment).getFirst();
                IteratorTripleString it = hdt.search(subj, "", "");
                if (it.hasNext()) {
                    typeInstancesSize++;
                    break;
                }
            }
        }*/
        return typeInstancesSize;
    }

    public long getTypeInstancesSize(String type, HDT hdt) throws IOException, NotFoundException {
        long typeInstancesSize = 0;
        Set<String> subjs = new HashSet<>();
        IteratorTripleString it = hdt.search("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", type);
        while (it.hasNext()) {
            TripleString ts = it.next();
            if (!subjs.contains(ts.getSubject().toString()))
                typeInstancesSize++;
            else subjs.add(ts.getSubject().toString());
        }
        return typeInstancesSize;
    }

    /**
     * Get all distinct predicates of a specific type
     *
     * @param type Type of class
     * @return typePredicates Set of predicates of type
     */
    public Set<String> getTypePredicates(String type) throws NotFoundException, IOException {
        Set<String> typePredicates = new HashSet<String>();
        for (String fragment : fragments.keySet()) {
            HDT hdt = fragments.get(fragment).getFirst();
            Set<String> preds = getTypePredicates(type, hdt);
            typePredicates.addAll(preds);
        }
        return typePredicates;
    }

    public Set<String> getTypePredicates(String type, HDT hdt) throws IOException, NotFoundException {
        Set<String> preds = new HashSet<>();
        IteratorTripleString it = hdt.search("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", type);
        while (it.hasNext()) {
            TripleString ts = it.next();
            String subj = ts.getSubject().toString();
            IteratorTripleString it2 = hdt.search(subj, "", "");
            while (it2.hasNext()) {
                TripleString ts2 = it2.next();
                if (!ts2.getPredicate().toString().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
                    preds.add(ts2.getPredicate().toString());
            }
        }
        return preds;
    }

    /**
     * Get distinct set of rdf:type
     *
     * @return types Set of rdf:types
     */
    private Set<String> getRDFTypes() throws NotFoundException, IOException {
        Set<String> types = new HashSet<String>();
        for (String fragment : fragments.keySet()) {
            HDT hdt = fragments.get(fragment).getFirst();
            types.addAll(getRDFTypes(hdt));
        }
        return types;
    }

    private Set<String> getRDFTypes(HDT hdt) throws NotFoundException {
        Set<String> types = new HashSet<>();
        IteratorTripleString it = hdt.search("", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "");
        while (it.hasNext()) {
            types.add(it.next().getObject().toString());
        }
        return types;
    }
}
