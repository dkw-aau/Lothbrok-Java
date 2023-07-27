package org.lothbrok.sparql;

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.colchain.colchain.community.CommunityMember;

import java.util.HashSet;
import java.util.Set;

public class LothbrokJenaConstants {
    private static final String BASE_URI = "http://colchain.org/";
    private static final String DATASET = "watdiv10M";
    public static final Resource LOTHBROK_GRAPH = ResourceFactory.createResource(BASE_URI+"fuseki#LothbrokGraph") ;
    public static long NTB = 0;
    public static int NEM = 0;
    public static int NRN = 0;
    public static int NRNBO = 0;
    public static int NRF = 0;
    public static int NRFBO = 0;
    public static int NIQ = 0;
    public static Set<CommunityMember> NODES_INVOLVED = new HashSet<>();
    public static int NODES = 1;
    public static int NODE = 0;
    public final static int BIND_NUM = 30;
    public static int INDEXED = 0;
    public static int LOCAL = 0;
    public static boolean DISTINCT = false;
    public final static int DOWNLOAD_THRESHOLD = 1000;
}
