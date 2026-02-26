package com.abdelwahab.query_worker.engine.spark;

import com.abdelwahab.query_worker.dto.query.FilterCondition;
import com.abdelwahab.query_worker.dto.query.OrderByClause;
import com.abdelwahab.query_worker.dto.query.QueryRequest;
import com.abdelwahab.query_worker.dto.query.SelectColumn;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Translates a structured {@link QueryRequest} into a Spark {@link Dataset} pipeline.
 *
 * <p><b>Responsibility (SRP):</b> one concern only — build the DataFrame.
 * Status updates, result persistence, and session management live elsewhere.
 *
 * <p><b>Query pipeline:</b>
 * <pre>
 *   Iceberg table
 *        │
 *        ├─ 1. WHERE filters
 *        ├─ 2. SELECT + optional aggregations
 *        ├─ 3. GROUP BY (when aggregations are present)
 *        ├─ 4. ORDER BY
 *        └─ 5. LIMIT (hard-capped at 10,000)
 * </pre>
 *
 * @see SparkService — the orchestrator that calls this builder
 */
public class SparkQueryBuilder {

    private static final Logger log = LoggerFactory.getLogger(SparkQueryBuilder.class);

    private final SparkSession spark;

    public SparkQueryBuilder(SparkSession spark) {
        this.spark = spark;
    }

    /**
     * Builds the result Dataset from the Iceberg table and the query definition.
     *
     * @param icebergTable fully-qualified table name, e.g. {@code iceberg.myproject.sales}
     * @param request      parsed query definition
     * @return result Dataset, not yet collected or written
     */
    public Dataset<Row> build(String icebergTable, QueryRequest request) {
        log.info("Building Spark query — table: {}", icebergTable);

        Dataset<Row> df = spark.read().format("iceberg").load(icebergTable);

        // ── 1. WHERE ──────────────────────────────────────────────────────────
        if (request.getFilters() != null && !request.getFilters().isEmpty()) {
            df = applyFilters(df, request.getFilters());
        }

        // ── 2 & 3. SELECT + GROUP BY / AGG ───────────────────────────────────
        df = applySelectAndGroupBy(df, request);

        // ── 4. ORDER BY ───────────────────────────────────────────────────────
        if (request.getOrderBy() != null && !request.getOrderBy().isEmpty()) {
            df = applyOrderBy(df, request.getOrderBy());
        }

        // ── 5. LIMIT ──────────────────────────────────────────────────────────
        if (request.getLimit() != null && request.getLimit() > 0) {
            df = df.limit(request.getLimit());
        }

        log.info("Query built — schema: {}", df.schema().simpleString());
        return df;
    }

    // ── Private helpers ────────────────────────────────────────────────────────

    /**
     * Applies SELECT columns, handling three cases:
     * <ol>
     *   <li><b>GROUP BY + aggregations</b> — {@code groupBy().agg(...)}</li>
     *   <li><b>Global aggregation</b> — {@code agg(...)} with no group key</li>
     *   <li><b>Plain SELECT</b> — {@code select(...)}, or a no-op for {@code *}</li>
     * </ol>
     */
    private Dataset<Row> applySelectAndGroupBy(Dataset<Row> df, QueryRequest request) {
        List<SelectColumn> selects = request.getSelect();

        // Partition selects into plain columns and aggregated columns
        List<SelectColumn> plainCols = selects.stream()
                .filter(s -> s.getAggregation() == null || s.getAggregation().isBlank())
                .collect(Collectors.toList());

        List<SelectColumn> aggCols = selects.stream()
                .filter(s -> s.getAggregation() != null && !s.getAggregation().isBlank())
                .collect(Collectors.toList());

        boolean hasAgg     = !aggCols.isEmpty();
        boolean hasGroupBy = request.getGroupBy() != null && !request.getGroupBy().isEmpty();

        if (hasAgg && hasGroupBy) {
            // GROUP BY columns + aggregation expressions
            Column[] groupByCols = request.getGroupBy().stream()
                    .map(functions::col)
                    .toArray(Column[]::new);

            Column[] aggExpressions = buildAggExpressions(aggCols);

            df = df.groupBy(groupByCols)
                   .agg(aggExpressions[0], Arrays.copyOfRange(aggExpressions, 1, aggExpressions.length));

            // Rename any plain (group-key) column that has an alias in the SELECT
            for (SelectColumn s : plainCols) {
                if (s.getAs() != null && !s.getAs().isBlank()) {
                    df = df.withColumnRenamed(s.getColumn(), s.getAs());
                }
            }

        } else if (hasAgg) {
            // Global aggregation — no GROUP BY
            Column[] aggExpressions = buildAggExpressions(aggCols);
            df = df.agg(aggExpressions[0], Arrays.copyOfRange(aggExpressions, 1, aggExpressions.length));

        } else {
            // Plain SELECT — wildcard is a no-op (keeps all columns)
            boolean isSelectAll = selects.size() == 1 && "*".equals(selects.get(0).getColumn());
            if (!isSelectAll) {
                Column[] selectCols = selects.stream()
                        .map(s -> {
                            Column c = functions.col(s.getColumn());
                            return (s.getAs() != null && !s.getAs().isBlank()) ? c.alias(s.getAs()) : c;
                        })
                        .toArray(Column[]::new);
                df = df.select(selectCols);
            }
        }

        return df;
    }

    private Column[] buildAggExpressions(List<SelectColumn> aggCols) {
        return aggCols.stream()
                .map(s -> {
                    Column expr = applyAggregation(functions.col(s.getColumn()), s.getAggregation());
                    return (s.getAs() != null && !s.getAs().isBlank())
                            ? expr.alias(s.getAs())
                            : expr.alias(s.getColumn());
                })
                .toArray(Column[]::new);
    }

    private Dataset<Row> applyFilters(Dataset<Row> df, List<FilterCondition> filters) {
        for (FilterCondition f : filters) {
            Column col = functions.col(f.getColumn());
            Object val = f.getValue();

            df = switch (f.getOperator()) {
                case "="           -> df.filter(col.equalTo(val));
                case "!="          -> df.filter(col.notEqual(val));
                case ">"           -> df.filter(col.gt(val));
                case ">="          -> df.filter(col.geq(val));
                case "<"           -> df.filter(col.lt(val));
                case "<="          -> df.filter(col.leq(val));
                case "LIKE"        -> df.filter(col.like(val.toString()));
                case "IN"          -> df.filter(col.isin(toObjectArray(val)));
                case "IS NULL"     -> df.filter(col.isNull());
                case "IS NOT NULL" -> df.filter(col.isNotNull());
                default -> {
                    log.warn("Unsupported filter operator '{}' on column '{}' — skipping",
                            f.getOperator(), f.getColumn());
                    yield df;
                }
            };
        }
        return df;
    }

    private Column applyAggregation(Column col, String aggregation) {
        return switch (aggregation.toLowerCase()) {
            case "sum"      -> functions.sum(col);
            case "avg"      -> functions.avg(col);
            case "count"    -> functions.count(col);
            case "min"      -> functions.min(col);
            case "max"      -> functions.max(col);
            case "stddev"   -> functions.stddev(col);
            case "variance" -> functions.variance(col);
            default -> {
                log.warn("Unsupported aggregation '{}' — treating column as plain", aggregation);
                yield col;
            }
        };
    }

    private Dataset<Row> applyOrderBy(Dataset<Row> df, List<OrderByClause> orderBy) {
        Column[] cols = orderBy.stream()
                .map(o -> "desc".equalsIgnoreCase(o.getDirection())
                        ? functions.col(o.getColumn()).desc()
                        : functions.col(o.getColumn()).asc())
                .toArray(Column[]::new);
        return df.orderBy(cols);
    }

    /** Converts a Jackson-deserialized value (scalar or {@code List<?>}) to an Object array for {@code isin}. */
    private Object[] toObjectArray(Object value) {
        if (value instanceof List<?> list) {
            return list.toArray();
        }
        return new Object[]{value};
    }
}
