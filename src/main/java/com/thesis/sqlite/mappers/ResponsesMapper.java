package com.thesis.sqlite.mappers;

import com.thesis.sqlite.components.query.base.Join;
import com.thesis.sqlite.dto.join.JoinDto;
import com.thesis.sqlite.dto.join.RelationDto;

import java.util.List;

public class ResponsesMapper {
    public static List<JoinDto> convert(List<Join> joins) {
        return joins.stream()
                .map(ResponsesMapper::convert)
                .toList();
    }

    public static JoinDto convert(Join join) {
        RelationDto rhs = new RelationDto(join.rhs.baseUrl, join.rhs.name, join.rhs.shortName, join.rhs.rowCount);
        RelationDto lhs = new RelationDto(join.lhs.baseUrl, join.lhs.name, join.lhs.shortName, join.lhs.rowCount);

        return new JoinDto(lhs, rhs, join.joinType, join.predicate);
    }
}
