package org.lothbrok.stars;

import org.lothbrok.utils.Tuple;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.TripleID;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public class StarID  implements Comparable<StarID>, Serializable {
    private static final long serialVersionUID = -4685524566493494912L;

    private int subject;
    private List<Tuple<Integer, Integer>> triples = new ArrayList<>();
    private String subjVar = "";
    private List<Tuple<String, String>> vars = new ArrayList<>();

    /**
     * Basic constructor
     */
    public StarID() {
        super();
    }

    public StarID(int subject, List<Tuple<Integer, Integer>> triples) {
        this.subject = subject;
        this.triples = triples;
    }

    public StarID(List<TripleID> lst) {
        this.subject = (int)lst.get(0).getSubject();
        this.triples = new ArrayList<>();
        for(TripleID tid : lst) {
            this.triples.add(new Tuple<>((int) tid.getPredicate(), (int) tid.getObject()));
        }
    }

    public StarID(int subject, List<Tuple<Integer, Integer>> triples, String subjVar, List<Tuple<String, String>> vars) {
        this(subject, triples);
        this.subjVar = subjVar;
        this.vars = vars;
    }

    public StarID(int subject) {
        this.subject = subject;
    }

    /**
     * Build a TripleID as a copy of another one.
     * @param other
     */
    public StarID(StarID other) {
        super();
        subject = other.subject;
        triples = new ArrayList<>(other.triples);
        subjVar = other.subjVar;
        vars = new ArrayList<>(other.vars);
    }

    public int size() {
        return triples.size();
    }

    public TripleID getTriple(int pos) {
        Tuple<Integer, Integer> t = triples.get(pos);
        return new TripleID(subject, t.x, t.y);
    }

    public List<Integer> getPredicates() {
        List<Integer> ret = new ArrayList<>();

        for(Tuple<Integer, Integer> t : triples) {
            ret.add(t.x);
        }

        return ret;
    }

    public int getSubject() {
        return subject;
    }

    public void setSubject(int subject) {
        this.subject = subject;
    }

    public void setObject(int predicate, int object) {
        boolean contains = false;

        for(Tuple<Integer, Integer> t : triples) {
            if(t.x == predicate) {
                contains = true;
                t.y = object;
                break;
            }
        }

        if(!contains) {
            triples.add(new Tuple<>(predicate, object));
        }
    }

    public TripleID remove(int pos) {
        vars.remove(pos);
        Tuple<Integer, Integer> tpl = triples.remove(pos);
        return new TripleID(subject, tpl.x, tpl.y);
    }

    public String getSubjVar() {
        return subjVar;
    }

    public String getPredVar(int pos) {
        return vars.get(pos).x;
    }

    public String getObjVar(int pos) {
        return vars.get(pos).y;
    }

    public boolean isEmpty() {
        return triples.isEmpty();
    }

    public List<Tuple<Integer, Integer>> getTriples() {
        return triples;
    }

    public Tuple<String, String> getVar(int pos) {
        return vars.get(pos);
    }

    public void setTriples(List<Tuple<Integer, Integer>> triples) {
        this.triples = triples;
    }

    public void addTriple(Tuple<Integer, Integer> triple) {
        triples.add(triple);
    }

    public StarString toStarString(Dictionary dictionary) {
        CharSequence subj = dictionary.idToString(subject, TripleComponentRole.SUBJECT);
        List<Tuple<CharSequence, CharSequence>> lst = new ArrayList<>();
        int size = size();
        for(int i = 0; i < size; i++) {
            TripleID tpl = getTriple(i);
            Tuple<CharSequence, CharSequence> t = new Tuple<>(
                    dictionary.idToString(tpl.getPredicate(), TripleComponentRole.PREDICATE),
                    dictionary.idToString(tpl.getObject(), TripleComponentRole.OBJECT)
            );
            lst.add(t);
        }

        return new StarString(subj, lst);
    }

    @Override
    public int compareTo(StarID other) {
        int result = this.subject - other.subject;

        if(result==0) {
            return triples.hashCode() - other.triples.hashCode();
        }
        return result;
    }

    @Override
    public String toString() {
        String str = subject + "";

        for(Tuple<Integer, Integer> t : triples) {
            str += "\n    " + t.x + " " + t.y;
        }

        return str;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        return this.hashCode() == o.hashCode();
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, new HashSet<>(triples));
    }

    /**
     * Set all components to zero.
     */
    public void clear() {
        triples.clear();
        subject = 0;
    }
}
