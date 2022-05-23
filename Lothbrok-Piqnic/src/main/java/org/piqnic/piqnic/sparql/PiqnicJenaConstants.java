package org.piqnic.piqnic.sparql;

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

public class PiqnicJenaConstants {
    private static final String BASE_URI = "http://piqnic.org/";
    public static final Resource PIQNIC_GRAPH = ResourceFactory.createResource(BASE_URI+"fuseki#PiqnicGraph") ;
    public static long NTB = 0;
    public static int NEM = 0;
    public final static int BIND_NUM = 30;
}
