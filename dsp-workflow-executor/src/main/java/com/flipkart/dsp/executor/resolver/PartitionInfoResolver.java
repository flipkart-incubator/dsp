package com.flipkart.dsp.executor.resolver;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.dsp.entities.misc.WhereClause;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.JsonUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

public class PartitionInfoResolver {

    public Set<ScriptVariable> resolve(String scope, Long refreshId, String stepName) {
        ArrayList<String> partitionsKeys = new ArrayList<>();
        ArrayList<String> partitionsValues = new ArrayList<>();
        partitionsKeys.add(Constants.REFRESH_ID);
        partitionsValues.add(refreshId.toString());
        partitionsKeys.add(Constants.STEP_NAME);
        partitionsValues.add(stepName);
        LinkedHashSet<WhereClause> whereClauses = null;
        TypeReference<LinkedHashSet<WhereClause>> typeRef = new TypeReference<LinkedHashSet<WhereClause>>() {
        };
        whereClauses = JsonUtils.DEFAULT.fromJson(scope, typeRef);
        for (WhereClause whereClause : whereClauses) {
            Set<String> values = whereClause.getValues();
            if (values != null) {
                for (String value : values) {
                    partitionsKeys.add(whereClause.getId());
                    partitionsValues.add(value);
                }
            }
        }

        return new HashSet<ScriptVariable>() {
            {
                add(new ScriptVariable(Constants.PARTITION_KEYS, DataType.ARRAY, partitionsKeys.toArray(new String[0]), null,  null, false));
                add(new ScriptVariable(Constants.PARTITION_VALUES, DataType.ARRAY, partitionsValues.toArray(new String[0]), null, null, false));
            }
        };
    }

}
