package org.lothbrok.characteristicset.impl;

import org.lothbrok.characteristicset.ICharacteristicSet;
import org.lothbrok.stars.StarString;
import org.lothbrok.utils.Tuple;

import java.util.*;

public class CharacteristicSetImpl implements ICharacteristicSet {
    private final Set<String> predicates;

    CharacteristicSetImpl(Set<String> predicates) {
        this.predicates = predicates;
    }

    CharacteristicSetImpl() {
        predicates = new HashSet<>();
    }

    @Override
    public List<String> getPredicates() {
        return new ArrayList<>(predicates);
    }

    @Override
    public boolean hasPredicates(Set<String> predicates) {
        return this.predicates.containsAll(predicates);
    }

    @Override
    public boolean hasPredicate(String predicate) {
        return this.predicates.contains(predicate);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CharacteristicSetImpl that = (CharacteristicSetImpl) o;
        return Objects.equals(predicates, that.predicates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(predicates);
    }

    @Override
    public StarString getAsStarString() {
        int i = 1;
        List<Tuple<CharSequence,CharSequence>> lst = new ArrayList<>();
        for(String p : predicates) {
            lst.add(new Tuple<>(p, "?v" + i));
            i++;
        }
        return new StarString("?v" + i, lst);
    }

    @Override
    public String toString() {
        return predicates.toString();
    }
}
