package com.thesis.sqlite.dto.join;

import com.thesis.sqlite.components.query.base.Attribute;
import com.thesis.sqlite.utils.Pair;

import java.util.List;

public class JoinDto {
    private RelationDto lhs;
    private RelationDto rhs;
    private String joinType;
    private List<Pair<Attribute, Attribute>> predicate;

    public JoinDto(RelationDto lhs, RelationDto rhs, String joinType, List<Pair<Attribute, Attribute>> predicate) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.joinType = joinType;
        this.predicate = predicate;
    }

    public RelationDto getLhs() {
        return lhs;
    }

    public void setLhs(RelationDto lhs) {
        this.lhs = lhs;
    }

    public RelationDto getRhs() {
        return rhs;
    }

    public void setRhs(RelationDto rhs) {
        this.rhs = rhs;
    }

    public String getJoinType() {
        return joinType;
    }

    public void setJoinType(String joinType) {
        this.joinType = joinType;
    }

    public List<Pair<Attribute, Attribute>> getPredicate() {
        return predicate;
    }

    public void setPredicate(List<Pair<Attribute, Attribute>> predicate) {
        this.predicate = predicate;
    }

    public String getJoinName() {
        return lhs.getShortName() + "_" + rhs.getShortName();
    }
}
