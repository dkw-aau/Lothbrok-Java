package org.lothbrok.characteristicset;

import org.lothbrok.stars.StarString;

import java.util.List;
import java.util.Set;

public interface ICharacteristicSet {
    StarString getAsStarString();
    boolean hasPredicates(Set<String> predicates);
    boolean hasPredicate(String predicates);
    List<String> getPredicates();
}
