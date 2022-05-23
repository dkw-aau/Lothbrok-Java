package org.rdfhdt.hdt.hdt.writer;

import org.rdfhdt.hdt.rdf.TripleWriter;

import java.io.IOException;
import java.io.OutputStream;

public class TripleWriterFactory {
	public static TripleWriter getWriter(String outFile, boolean compress) throws IOException {
		return new TripleWriterNtriples(outFile, compress);
	}
	
	public static TripleWriter getWriter(OutputStream out) {
		return new TripleWriterNtriples(out);
	}
}
