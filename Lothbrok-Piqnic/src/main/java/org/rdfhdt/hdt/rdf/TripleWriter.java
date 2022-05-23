package org.rdfhdt.hdt.rdf;

import org.rdfhdt.hdt.triples.TripleString;

import java.io.IOException;

public interface TripleWriter extends AutoCloseable {
	public void addTriple(TripleString str) throws IOException;
}
