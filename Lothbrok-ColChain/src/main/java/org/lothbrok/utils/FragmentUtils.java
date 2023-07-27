package org.lothbrok.utils;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.colchain.colchain.community.Community;
import org.colchain.colchain.community.CommunityMember;
import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.util.MergedHDTIterator;
import org.lothbrok.characteristicset.ICharacteristicSet;
import org.lothbrok.characteristicset.impl.CharacteristicSetFactory;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorStarString;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class FragmentUtils {
    private static final RandomString gen = new RandomString();
    private static final CloseableHttpClient httpClient = HttpClients.createDefault();
    public static void upload(String path, String community) throws IOException {
        Map<String, Set<String>> subjMap = new HashMap<>();

        System.out.println("Finding characteristic sets..");
        HDT hdt = HDTManager.mapIndexedHDT(path);

        IteratorTripleString iterator;
        try {
            iterator = hdt.search("", "", "");
        } catch (NotFoundException e) {
            e.printStackTrace();
            return;
        }

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
        }

        System.out.print("\n");
        System.out.println("Left with " + csSet.size() + " characteristic sets.");

        try {
            for (ICharacteristicSet cs : csSet) {
                System.out.println("    Handling CS " + cs.toString());

                String id = gen.nextString();
                String outpath = AbstractNode.getState().getDatastore() + "hdt/" + id + ".hdt";

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

                AbstractNode.getState().addNewFragment(id, id, outpath, community, AbstractNode.getState().getPublicKey(), cs);
                Community c = AbstractNode.getState().getCommunity(community);
                Set<CommunityMember> parts = c.getParticipants();
                for (CommunityMember m : parts) {
                    if(m.getAddress().equals(AbstractNode.getState().getAddress())) continue;
                    String a = m.getAddress() + (m.getAddress().endsWith("/") ? "" : "/") + "api/community?mode=newFragment&id="
                            + id + "&address=" + AbstractNode.getState().getAddress() + "&community=" + community + "&cs=" + cs;
                    performAction(a);
                }

                Set<CommunityMember> obs = c.getObservers();
                for (CommunityMember m : obs) {
                    if(m.getAddress().equals(AbstractNode.getState().getAddress())) continue;
                    String a = m.getAddress() + (m.getAddress().endsWith("/") ? "" : "/") + "api/community?mode=newFragment&id="
                            + id + "&address=" + AbstractNode.getState().getAddress() + "&community=" + community + "&cs=" + cs;
                    performAction(a);
                }
            }
        } catch (NotFoundException e) {
            e.printStackTrace();
            return;
        }
    }

    private static ICharacteristicSet getSuperset(ICharacteristicSet cs, Set<ICharacteristicSet> csSet) {
        for (ICharacteristicSet css : csSet) {
            if (cs.equals(css)) continue;
            if (css.hasPredicates(new HashSet<>(cs.getPredicates()))) return css;
        }

        return null;
    }

    private static void performAction(String address) {
        HttpGet request = new HttpGet(address);
        try {
            httpClient.execute(request);
        } catch (IOException e) {
        }
    }
}
