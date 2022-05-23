package org.lothbrok.strategy.dp;

import org.lothbrok.exceptions.SubStrategyNotFoundException;
import org.lothbrok.stars.StarString;
import org.lothbrok.strategy.IQueryStrategy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DpTable {
    private final List<DpTableEntry> entries = new ArrayList<>();

    public void addEntry(DpTableEntry entry) {
        entries.add(entry);
    }

    public List<DpTableEntry> getEntries() {
        return entries;
    }

    public IQueryStrategy getTopmostStrategy() {
        DpTableEntry entry = entries.get(entries.size()-1);
        Set<StarString> bgp = new HashSet<>(entry.getSubquery());
        IQueryStrategy lowestCost = entry.getStrategy();
        long cost = entry.getCost();
        for(int i = 0; i < entries.size()-1; i++) {
            DpTableEntry entry1 = entries.get(i);
            if(!bgp.equals(new HashSet<>(entry1.getSubquery()))) continue;
            if(entry1.getCost() < cost) {
                cost = entry1.getCost();
                lowestCost = entry1.getStrategy();
            }
        }
        return lowestCost;
    }

    public DpTableEntry getTopmostEntry() {
        return entries.get(entries.size()-1);
    }

    public List<DpTableEntry> getTopEntries() {
        List<DpTableEntry> ret = new ArrayList<>();
        int size = entries.get(entries.size()-1).getSubquery().size();
        for(DpTableEntry entry : entries) {
            if(entry.getSubquery().size() == size) ret.add(entry);
        }

        return ret;
    }

    public IQueryStrategy getStrategyBySubquery(List<StarString> subquery) {
        for(DpTableEntry entry : entries) {
            if(entry.isSubquery(subquery)) return entry.getStrategy();
        }
        throw new SubStrategyNotFoundException("Could not find substrategy for subquery.");
    }

    public boolean containsSubquery(List<StarString> bgp) {
        for(DpTableEntry entry : entries) {
            if(entry.isSubquery(bgp)) return true;
        }
        return false;
    }

    public DpTableEntry getEntryBySubquery(List<StarString> bgp) {
        for(DpTableEntry entry : entries) {
            if(entry.isSubquery(bgp)) return entry;
        }
        throw new SubStrategyNotFoundException("Did not find subquery.");
    }

    public void replaceStrategy(List<StarString> bgp, DpTableEntry entry) {
        int index = getEntryIndex(bgp);
        entries.remove(index);
        entries.add(index, entry);
    }

    private int getEntryIndex(List<StarString> bgp) {
        for(int i = 0; i < entries.size(); i++) {
            DpTableEntry entry = entries.get(i);
            if(entry.isSubquery(bgp)) return i;
        }
        throw new SubStrategyNotFoundException("Did not find subquery.");
    }

    @Override
    public String toString() {
        return "DpTable{" +
                "entries=" + entries +
                '}';
    }
}
