package com.flipkart.dsp.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.entities.misc.WhereClause;

import java.util.LinkedHashSet;

public class PartitionScopeUtil {
    private static final String DEFAULT_PARTITION = "default_partition";

    public String populateJobName(String scope) {
        TypeReference<LinkedHashSet<WhereClause>> typeRef = new TypeReference<LinkedHashSet<WhereClause>>() {};
        LinkedHashSet<WhereClause> whereClauses = JsonUtils.DEFAULT.fromJson(scope, typeRef);
        StringBuilder scopeBuilder = new StringBuilder();
        for(WhereClause clause : whereClauses) {
            scopeBuilder.append(clause.getValues());
            scopeBuilder.append("##");
        }
        if (scopeBuilder.length()>0) {
            scopeBuilder.replace(scopeBuilder.length()-2,scopeBuilder.length(),"");
        } else {
            scopeBuilder.append(DEFAULT_PARTITION);
        }
        return scopeBuilder.toString().replaceAll("%23","_");
    }
}
