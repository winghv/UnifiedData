package com.example.unifieddataservice.service;

import com.example.unifieddataservice.model.MetricInfo;
import com.example.unifieddataservice.model.Predicate;
import com.example.unifieddataservice.model.PushdownResult;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

/**
 * Service to analyze predicates and determine pushdown capability.
 */
@Service
public class PredicatePushdownService {

    /**
     * Analyzes a list of predicates against a given metric to determine
     * which can be pushed down to the data source.
     *
     * <p><b>Current Logic (Simple):</b> Assumes all predicates can be pushed down.
     * This can be extended to check data source capabilities, column existence, etc.</p>
     *
     * @param predicates The list of predicates from the query.
     * @param metricInfo The metric being queried.
     * @return A {@link PushdownResult} partitioning the predicates.
     */
    public PushdownResult analyze(List<Predicate> predicates, MetricInfo metricInfo) {
        if (predicates == null || predicates.isEmpty()) {
            return new PushdownResult(Collections.emptyList(), Collections.emptyList());
        }

        // For now, assume all predicates can be pushed down.
        // Future logic could inspect metricInfo.getDataSourceType() or other metadata.
        return new PushdownResult(predicates, Collections.emptyList());
    }
}
