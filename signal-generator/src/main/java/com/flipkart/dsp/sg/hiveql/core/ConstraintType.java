package com.flipkart.dsp.sg.hiveql.core;

import com.flipkart.dsp.models.sg.PredicateType;
import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Constraint;
import com.flipkart.dsp.sg.hiveql.query.HiveQueryConstants;
import com.flipkart.dsp.utils.Constants;

import java.util.Arrays;
import java.util.Optional;

import static com.flipkart.dsp.sg.hiveql.core.ColumnDataType.STRING;
import static com.flipkart.dsp.sg.hiveql.query.HiveQueryConstants.*;
import static com.flipkart.dsp.sg.hiveql.query.SelectQuery.UNIX_TIMESTAMP;
import static com.flipkart.dsp.utils.Constants.dot;
import static java.lang.String.format;

/**
 */
public enum ConstraintType {
    IN(PredicateType.IN) {
        @Override
        public String getPrefix() {
            return SINGLE_SPACE;
        }

        @Override
        public String getPostfix() {
            return HiveQueryConstants.IN;
        }

        @Override
        public String addConstraintValue(Constraint constraint) {
            return processInAndNotIn(constraint);
        }
    },

    NOT_IN(PredicateType.NOT_IN) {
        @Override
        public String getPrefix() {
            return SINGLE_SPACE;
        }

        @Override
        public String getPostfix() {
            return HiveQueryConstants.NOT_IN;
        }

        @Override
        public String addConstraintValue(Constraint constraint) {
            return processInAndNotIn(constraint).toString();
        }
    },

    EQUAL(PredicateType.EQUAL) {
        @Override
        public String getPrefix() {
            return "";
        }

        @Override
        public String getPostfix() {
            return Constants.equal;
        }

        @Override
        public String addConstraintValue(Constraint constraint) {
            return getConstraintValue(constraint.getConstraintColumn(), constraint.getConstraintValue());
        }
    },

    RANGE(PredicateType.RANGE) {
        @Override
        public String getPrefix() {
            return "";
        }

        @Override
        public String getPostfix() {
            return BETWEEN;
        }

        @Override
        public String addConstraintValue(Constraint constraint) {
            return processRange(constraint);
        }
    },

    LESS_THAN(PredicateType.LESS_THAN) {
        @Override
        public String getPrefix() {
            return "";
        }

        @Override
        public String getPostfix() {
            return "<";
        }

        @Override
        public String addConstraintValue(Constraint constraint) {
            return constraint.getConstraintValue().toString();
        }
    },

    GREATER_THAN(PredicateType.GREATER_THAN) {
        @Override
        public String getPrefix() {
            return "";
        }

        @Override
        public String getPostfix() {
            return ">";
        }

        @Override
        public String addConstraintValue(Constraint constraint) {
            return constraint.getConstraintValue().toString();
        }
    },

    TIME_YYYYWW_RANGE(PredicateType.TIME_YYYYWW_RANGE) {
        @Override
        public String getPrefix() {
            return "";
        }

        @Override
        public String getPostfix() {
            return BETWEEN;
        }

        @Override
        public String addConstraintValue(Constraint constraint) {
            return processRange(constraint);
        }
    },

    TIME_YYYYMMDD_RANGE(PredicateType.TIME_YYYYMMDD_RANGE) {
        @Override
        public String getPrefix() {
            return UNIX_TIMESTAMP + OPEN_BRACKET;
        }

        @Override
        public String getPostfix() {
            return COMMA + YYYY_MM_DD + CLOSE_BRACKET;
        }

        @Override
        public String addConstraintValue(Constraint constraint) {
            return processDateRange(constraint);
        }
    },

    INCREMENTAL_DATE_RANGE(PredicateType.INCREMENTAL_DATE_RANGE) {
        @Override
        public String getPrefix() {
            return UNIX_TIMESTAMP + OPEN_BRACKET;
        }

        @Override
        public String getPostfix() {
            return SINGLE_SPACE;
        }

        @Override
        public String addConstraintValue(Constraint constraint) {
            return processDateRange(constraint);
        }
    },

    INCREMENTAL_WEEK_RANGE(PredicateType.INCREMENTAL_WEEK_RANGE) {
        @Override
        public String getPrefix() {
            return "";
        }

        @Override
        public String getPostfix() {
            return BETWEEN;
        }

        @Override
        public String addConstraintValue(Constraint constraint) {
            return processRange(constraint);
        }
    };

    private PredicateType predicateType;

    ConstraintType(PredicateType predicateType) {
        this.predicateType = predicateType;
    }

    public static ConstraintType from(PredicateType predicateType) {
        Optional<ConstraintType> constraintTypeOptional = Arrays.stream(ConstraintType.values())
                .filter(c -> c.getPredicateType().equals(predicateType)).findFirst();

        if (!constraintTypeOptional.isPresent())
            throw new RuntimeException("Failed to map SignalDefinitionScopeType to ConstraintType !! Predicate type : " + predicateType);

        return constraintTypeOptional.get();
    }

    private static String getConstraintValue(Column column, Object constraintValue) {
        if (STRING.equals(column.getColumnDataType()))
            return format("'%s'", constraintValue.toString());
        return constraintValue.toString();
    }

    public static String getConstraints(String table, Constraint constraint) {
        return constraint.getConstraintType().getPrefix() + table + DOT + constraint.getConstraintColumn().getColumnName()
                + SINGLE_SPACE + constraint.getConstraintType().getPostfix() + SINGLE_SPACE
                + constraint.getConstraintType().addConstraintValue(constraint);
    }

    private static String processRange(Constraint constraint) {
        return constraint.getRange().getValue0() + SINGLE_SPACE + AND + SINGLE_SPACE + constraint.getRange().getValue1();
    }

    private static String processDateRange(Constraint constraint) {
        return BETWEEN + SINGLE_SPACE + UNIX_TIMESTAMP + OPEN_BRACKET + "'" + constraint.getRange().getValue0() + "'"
                + COMMA + YYYY_MM_DD + CLOSE_BRACKET + SINGLE_SPACE + AND + SINGLE_SPACE + UNIX_TIMESTAMP + OPEN_BRACKET
                + "'" + constraint.getRange().getValue1() + "'" + COMMA + YYYY_MM_DD + CLOSE_BRACKET;
    }

    public String addTableName(String tableName) {
        return tableName + dot;
    }

    private static String processInAndNotIn(Constraint constraint) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(OPEN_BRACKET);
        boolean firstIteration = true;
        for (Object value : constraint.getInValues()) {
            if (!firstIteration)
                stringBuilder.append(COMMA);
            firstIteration = false;
            stringBuilder.append(getConstraintValue(constraint.getConstraintColumn(), value));
        }
        stringBuilder.append(CLOSE_BRACKET);
        return stringBuilder.toString();
    }

    public abstract String getPostfix();

    public abstract String getPrefix();

    public abstract String addConstraintValue(Constraint constraint);


    public PredicateType getPredicateType() {
        return predicateType;
    }
}
