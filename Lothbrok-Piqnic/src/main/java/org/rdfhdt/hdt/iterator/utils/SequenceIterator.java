package org.rdfhdt.hdt.iterator.utils;

import org.rdfhdt.hdt.compact.sequence.Sequence;

import java.util.Iterator;

public class SequenceIterator implements Iterator<Long> {

	Sequence seq;
	long pos, max;
	
	public SequenceIterator(Sequence seq) {
		this.seq = seq;
		this.pos = 0;
		this.max = seq.getNumberOfElements();
	}
	
	public SequenceIterator(Sequence seq, long min, long max) {
		this.pos = min;
		this.max = Math.min(seq.getNumberOfElements(), max);
	}
	
	@Override
	public boolean hasNext() {
		return pos<max;
	}

	@Override
	public Long next() {
		return seq.get(pos++);
	}

	@Override
	public void remove() {
		
	}

}
