package com.thesis.sqlite.components.query.base;

import com.thesis.sqlite.utils.Pair;

import java.util.ArrayList;

public class Join implements Comparable<Join> {

    public Relation lhs;
    public Relation rhs;
    public ArrayList<Pair<Attribute, Attribute>> predicate;
    public String joinType;
    //in DBMS units
    public double localCost;


    public Join() {
        predicate = new ArrayList<>();
        this.joinType = "";
        this.localCost = 0;
    }

    public void addPredicate(Attribute lhs, Attribute rhs) {
        Pair<Attribute, Attribute> pred = new Pair<>(lhs, rhs);
        this.predicate.add(pred);

    }

    public void addPredicates(ArrayList<Pair<Attribute, Attribute>> preds) {
        this.predicate.addAll(preds);
    }

//    @Override
//    public String toString() {
//        return onDbms.name + " joins [" + lhs.dbms.name + "]" + lhs.shortName + " (size: " + lhs.rowCount + ") and " +
//                "[" + rhs.dbms.name + "]" + rhs.shortName + " (size: " + rhs.rowCount + ") on " + getJoinPredStr() +
//                " with cost: " + localCost + " and mode: " + joinType;
//        //return lhs.shortName+" joins "+rhs.shortName+" on " + getJoinPredStr();
//    }

    public String getJoinPredStr() {
        StringBuilder sb = new StringBuilder();
        for (Pair<Attribute, Attribute> p : this.predicate) {
            sb.append(p.getKey().getName());
            sb.append("=");
            sb.append(p.getValue().getName());
            sb.append(" AND ");
        }
        sb.append("1=1");
        return sb.toString();

    }

    public String getSQL() {
        return "SELECT * FROM " + this.lhs.shortName + "," + this.rhs.shortName + " WHERE " + this.getJoinPredStr();
    }

    public String getJoinName() {
        return lhs.shortName + "_" + rhs.shortName;
    }

    @Override
    public int compareTo(Join j) {
        return (int) (this.localCost - j.localCost);
    }
}
