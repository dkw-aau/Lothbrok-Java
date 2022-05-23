package org.lothbrok.utils;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.colchain.colchain.community.Community;
import org.colchain.colchain.community.CommunityMember;
import org.colchain.colchain.node.AbstractNode;
import org.lothbrok.characteristicset.ICharacteristicSet;
import org.lothbrok.characteristicset.impl.CharacteristicSetFactory;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorStarString;
import org.rdfhdt.hdt.triples.IteratorTripleString;

import java.io.IOException;
import java.util.*;

public class FragmentUtils {
    private static RandomString gen = new RandomString();
    private static final CloseableHttpClient httpClient = HttpClients.createDefault();
    public static void upload(String path, String community) throws IOException {
        System.out.println("Uploading " + path);
        HDT hdt = HDTManager.mapIndexedHDT(path, null);

        DictionarySection subjects = hdt.getDictionary().getSubjects();
        System.out.println("Found " + subjects.getNumberOfElements() + " subjects.");

        Set<ICharacteristicSet> families = new HashSet<>();
        int count = subjects.getNumberOfElements();
        int num = 1;
        Iterator<? extends CharSequence> it = subjects.getSortedEntries();
        while(it.hasNext()) {
            CharSequence subj = it.next();
            System.out.print("\rSubject " + num + "/" + count);
            num++;

            Set<String> family = new HashSet<>();
            IteratorTripleString triples;
            try {
                triples = hdt.search(subj, "", "");
                while(triples.hasNext()) {
                    family.add(triples.next().getPredicate().toString());
                }
            } catch (NotFoundException e) {
                continue;
            }

            families.add(CharacteristicSetFactory.create(family));
        }

        System.out.println("\nFound " + families.size() + " families");
        //System.out.println(families);

        for(ICharacteristicSet family : families) {
            System.out.println("Saving characteristic set " + family.toString());

            String id = gen.nextString();
            String outpath = AbstractNode.getState().getDatastore() + "hdt/" + id + ".hdt";

            IteratorStarString triples = hdt.searchStar(family.getAsStarString());
            HDT newHdt;
            try {
                newHdt = HDTManager.generateHDT(new FragmentGeneratorIterator(triples), "http://colchain.org/fragments#" + id, new HDTSpecification(), null);
            } catch (Exception e) {
                continue;
            }

            newHdt.saveToHDT(outpath, null);
            System.out.println("Saved file: " + outpath);

            newHdt.close();

            AbstractNode.getState().addNewFragment(id, id, outpath, community, AbstractNode.getState().getPublicKey(), family);
            Community c = AbstractNode.getState().getCommunity(community);
            Set<CommunityMember> parts = c.getParticipants();
            for (CommunityMember m : parts) {
                if(m.getAddress().equals(AbstractNode.getState().getAddress())) continue;
                String a = m.getAddress() + (m.getAddress().endsWith("/") ? "" : "/") + "api/community?mode=newFragment&id="
                        + id + "&address=" + AbstractNode.getState().getAddress() + "&community=" + community + "&cs=" + family;
                performAction(a);
            }

            Set<CommunityMember> obs = c.getObservers();
            for (CommunityMember m : obs) {
                if(m.getAddress().equals(AbstractNode.getState().getAddress())) continue;
                String a = m.getAddress() + (m.getAddress().endsWith("/") ? "" : "/") + "api/community?mode=newFragment&id="
                        + id + "&address=" + AbstractNode.getState().getAddress() + "&community=" + community + "&cs=" + family;
                performAction(a);
            }
        }
    }

    private static void performAction(String address) {
        HttpGet request = new HttpGet(address);
        try {
            httpClient.execute(request);
        } catch (IOException e) {
        }
    }
}
