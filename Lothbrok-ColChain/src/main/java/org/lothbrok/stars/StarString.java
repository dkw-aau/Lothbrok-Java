package org.lothbrok.stars;

import org.lothbrok.utils.Tuple;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.triples.TripleString;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class StarString {
    private CharSequence subject;
    private List<Tuple<CharSequence, CharSequence>> triples = new ArrayList<>();

    /**
     * Basic constructor
     */
    public StarString() {
        super();
    }

    public StarString(CharSequence subject, List<Tuple<CharSequence, CharSequence>> triples) {
        this.subject = subject;
        for(Tuple<CharSequence, CharSequence> tpl : triples) {
            this.triples.add(new Tuple<>(tpl.x, tpl.y));
        }
    }

    public Tuple<CharSequence, CharSequence> getTripleAt(int index) {
        return triples.get(index);
    }

    public StarString(CharSequence subject) {
        this.subject = subject;
    }

    /**
     * Build a TripleID as a copy of another one.
     * @param other
     */
    public StarString(StarString other) {
        super();
        subject = other.subject;
        triples = other.triples;
    }

    public Tuple<CharSequence, CharSequence> getFirstTripleWithPredicate(String pred) {
        for(Tuple<CharSequence, CharSequence> tpl : triples) {
            if(tpl.x.toString().equals(pred)) return tpl;
        }
        return triples.get(0);
    }

    public TripleString getTriple(int pos) {
        Tuple<CharSequence, CharSequence> t = triples.get(pos);
        return new TripleString(subject, t.x, t.y);
    }

    public TripleString getTripleString(int pos) {
        Tuple<CharSequence, CharSequence> t = triples.get(pos);
        return new TripleString(subject.toString().startsWith("?")? "" : subject,
                t.x.toString().startsWith("?")? "" : t.x,
                t.y.toString().startsWith("?")? "" : t.y);
    }

    public ArrayList<TripleString> toTripleStrings() {
        ArrayList<TripleString> ret = new ArrayList<>();
        for (Tuple<CharSequence, CharSequence> t : triples) {
            ret.add(new TripleString(subject, t.x, t.y));
        }
        return ret;
    }

    public List<String> getPredicates() {
        List<String> ret = new ArrayList<>();

        for(Tuple<CharSequence, CharSequence> t : triples) {
            ret.add(t.x.toString());
        }

        return ret;
    }

    public int size() {
        return triples.size();
    }

    public CharSequence getSubject() {
        return subject;
    }

    public void setSubject(CharSequence subject) {
        this.subject = subject;
    }

    public List<Tuple<CharSequence, CharSequence>> getTriples() {
        return triples;
    }

    public void setTriples(List<Tuple<CharSequence, CharSequence>> triples) {
        this.triples = triples;
    }

    public void addTriple(Tuple<CharSequence, CharSequence> triple) {
        triples.add(triple);
    }

    public StarID toStarID(Dictionary dictionary) {
        int subj = (subject.equals("") || subject.charAt(0) == '?')? 0 : (int)dictionary.stringToId(subject, TripleComponentRole.SUBJECT);
        String subjVar = (subject.equals("") || subject.charAt(0) == '?')? subject.toString() : "";

        List<Tuple<String, String>> vars = new ArrayList<>();
        List<Tuple<Integer, Integer>> lst = new ArrayList<>();
        int size = size();
        for(int i = 0; i < size; i++) {
            TripleString tpl = getTriple(i);
            Tuple<Integer, Integer> t = new Tuple<>(
                    (tpl.getPredicate().equals("") || tpl.getPredicate().charAt(0) == '?')? 0 : (int)dictionary.stringToId(tpl.getPredicate(), TripleComponentRole.PREDICATE),
                    (tpl.getObject().equals("") || tpl.getObject().charAt(0) == '?')? 0 : (int)dictionary.stringToId(tpl.getObject(), TripleComponentRole.OBJECT)
            );
            lst.add(t);

            Tuple<String, String> t1 = new Tuple<>(
                    tpl.getPredicate().charAt(0) == '?'? tpl.getPredicate().toString() : "",
                    tpl.getObject().charAt(0) == '?'? tpl.getObject().toString() : ""
            );
            vars.add(t1);
        }

        return new StarID(subj, lst, subjVar, vars);
    }

    public void updateField(String name, String val) {
        String vname;
        if(name.startsWith("?")) vname = name;
        else vname = "?" + name;

        if(name.equals("subject") || subject.toString().equals(vname)) {
            subject = val;
            return;
        }

        for(Tuple<CharSequence, CharSequence> tpl : triples) {
            if(tpl.x.toString().equals(vname)) tpl.x = val;
            if(tpl.y.toString().equals(vname)) tpl.y = val;
        }

        /*String so = name.substring(0,1);
        int num = Integer.parseInt(name.substring(1));

        if(so.equals("p"))
            triples.get(num-1).x = val;
        else
            triples.get(num-1).y = val;*/
    }

    @Override
    public String toString() {
        String str = subject.toString();

        for(Tuple<CharSequence, CharSequence> t : triples) {
            str += "\n    " + t.x + " " + t.y;
        }

        return str;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StarString starID = (StarString) o;
        return subject == starID.subject &&
                Objects.equals(triples, starID.triples);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, triples);
    }

    /**
     * Set all components to zero.
     */
    public void clear() {
        triples.clear();
        subject = "";
    }

    public List<String> getVariables() {
        List<String> vars = new ArrayList<>();
        if(subject.toString().startsWith("?"))
            vars.add(subject.toString());

        for(Tuple<CharSequence, CharSequence> tpl : triples) {
            String pred = tpl.x.toString(), obj = tpl.y.toString();
            if(pred.startsWith("?")) vars.add(pred);
            if(obj.startsWith("?")) vars.add(obj);
        }

        return vars;
    }

    public int numBoundVars(List<String> bound) {
        int bv = 0;
        if(bound.contains(subject.toString())) bv++;

        for(Tuple<CharSequence, CharSequence> tpl : triples) {
            String pred = tpl.x.toString(), obj = tpl.y.toString();
            if(bound.contains(pred)) bv++;
            if(bound.contains(obj)) bv++;
        }

        return bv;
    }

    public int numBoundSO(List<String> bound) {
        int bso = 0;
        if(bound.contains(subject.toString()) || !subject.toString().startsWith("?")) bso++;

        for(Tuple<CharSequence, CharSequence> tpl : triples) {
            String obj = tpl.y.toString();
            if(bound.contains(obj) || !obj.startsWith("?")) bso++;
        }

        return bso;
    }

    public int numBoundSO() {
        int bso = 0;
        if(!subject.toString().startsWith("?")) bso++;

        for(Tuple<CharSequence, CharSequence> tpl : triples) {
            String obj = tpl.y.toString();
            if(!obj.startsWith("?")) bso++;
        }

        return bso;
    }
}
