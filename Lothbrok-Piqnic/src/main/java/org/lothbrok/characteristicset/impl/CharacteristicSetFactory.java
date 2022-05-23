package org.lothbrok.characteristicset.impl;

import org.lothbrok.characteristicset.ICharacteristicSet;

import java.util.Set;

public class CharacteristicSetFactory {
    public static ICharacteristicSet create(Set<String> predicates) {
        return new CharacteristicSetImpl(predicates);
    }

    public static ICharacteristicSet create() {
        return new CharacteristicSetImpl();
    }
}
