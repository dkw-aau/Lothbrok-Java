package org.lothbrok.sparql.graph;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.Mode;
import org.apache.jena.assembler.assemblers.AssemblerBase;
import org.apache.jena.assembler.exceptions.AssemblerException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.lothbrok.sparql.LothbrokJenaConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LothbrokGraphAssembler extends AssemblerBase implements Assembler {
    private static final Logger log = LoggerFactory.getLogger(LothbrokGraphAssembler.class);
    private static boolean initialized;

    public static void init() {
        if(initialized) {
            return;
        }

        initialized = true;

        Assembler.general.implementWith(LothbrokJenaConstants.LOTHBROK_GRAPH, new LothbrokGraphAssembler());
    }

    @Override
    public Model open(Assembler a, Resource root, Mode mode)
    {
        try {
            LothbrokGraph graph = new LothbrokGraph();
            return ModelFactory.createModelForGraph(graph);
        } catch (Exception e) {
            log.error("Error creating graph: {}", e);
            throw new AssemblerException(root, "Error creating graph / "+e.toString());
        }
    }

    static {
        init();
    }
}
