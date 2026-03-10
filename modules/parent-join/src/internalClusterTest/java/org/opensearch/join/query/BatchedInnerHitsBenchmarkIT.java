/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.join.query;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.search.SearchService;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.join.query.JoinQueryBuilders.hasChildQuery;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.lucene.search.join.ScoreMode;

/**
 * Benchmark test for batched inner hits.
 * Run manually to compare performance between sequential and batched execution.
 *
 * Usage:
 *   ./gradlew :modules:parent-join:internalClusterTest \
 *     --tests "org.opensearch.join.query.BatchedInnerHitsBenchmarkIT" --info
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class BatchedInnerHitsBenchmarkIT extends ParentChildTestCase {

    private static final int WARMUP_ITERATIONS = 3;
    private static final int MEASURE_ITERATIONS = 10;

    private final boolean batchEnabled;

    public BatchedInnerHitsBenchmarkIT(Settings settings) {
        super(settings);
        this.batchEnabled = settings.getAsBoolean(SearchService.INNER_HITS_BATCH_ENABLED_SETTING.getKey(), false);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(SearchService.INNER_HITS_BATCH_ENABLED_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(SearchService.INNER_HITS_BATCH_ENABLED_SETTING.getKey(), true).build() }
        );
    }

    /**
     * Scenario 1: 500 parents × 50 children, many segments (no force merge).
     * This is the scenario where batching should show the most improvement.
     */
    public void testLargeScaleManySegments() throws Exception {
        int numParents = 500;
        int childrenPerParent = 50;
        int searchSize = 500;

        assertAcked(
            prepareCreate("bench_large").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                    .field("parent", "child")
                    .endObject()
                    .endObject()
                    .startObject("value")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
            ).setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    // Small refresh interval to create many segments
                    .put("index.refresh_interval", "1s")
            )
        );

        // Index in batches to create multiple segments
        for (int batch = 0; batch < 10; batch++) {
            List<IndexRequestBuilder> requests = new ArrayList<>();
            int startParent = batch * (numParents / 10);
            int endParent = startParent + (numParents / 10);
            for (int p = startParent; p < endParent; p++) {
                String parentId = "p" + p;
                requests.add(createIndexRequest("bench_large", "parent", parentId, null, "value", "parent_" + p));
                for (int c = 0; c < childrenPerParent; c++) {
                    requests.add(createIndexRequest("bench_large", "child", "c" + p + "_" + c, parentId, "value", "child_" + c));
                }
            }
            indexRandom(true, requests);
        }
        ensureGreen("bench_large");

        // Don't force merge — keep many segments
        refresh("bench_large");

        // Log segment count for reference
        var segmentsResponse = client().admin().indices().prepareSegments("bench_large").get();
        logger.info("bench_large segments response: {}", segmentsResponse);

        runBenchmark("bench_large", numParents, childrenPerParent, searchSize);
    }

    /**
     * Scenario 2: 100 parents × 10 children, 5 segments (original scenario for comparison).
     */
    public void testSmallScaleFewSegments() throws Exception {
        int numParents = 100;
        int childrenPerParent = 10;
        int searchSize = 100;

        assertAcked(
            prepareCreate("bench_small").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("join_field")
                    .field("type", "join")
                    .startObject("relations")
                    .field("parent", "child")
                    .endObject()
                    .endObject()
                    .startObject("value")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
            ).setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );

        List<IndexRequestBuilder> requests = new ArrayList<>();
        for (int p = 0; p < numParents; p++) {
            String parentId = "p" + p;
            requests.add(createIndexRequest("bench_small", "parent", parentId, null, "value", "parent_" + p));
            for (int c = 0; c < childrenPerParent; c++) {
                requests.add(createIndexRequest("bench_small", "child", "c" + p + "_" + c, parentId, "value", "child_" + c));
            }
        }
        indexRandom(true, requests);
        ensureGreen("bench_small");
        client().admin().indices().prepareForceMerge("bench_small").setMaxNumSegments(5).get();
        refresh("bench_small");

        runBenchmark("bench_small", numParents, childrenPerParent, searchSize);
    }

    private void runBenchmark(String index, int numParents, int childrenPerParent, int searchSize) {
        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            executeQuery(index, searchSize);
        }

        // Measure
        long totalTime = 0;
        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        for (int i = 0; i < MEASURE_ITERATIONS; i++) {
            long start = System.nanoTime();
            SearchResponse response = executeQuery(index, searchSize);
            long elapsed = System.nanoTime() - start;
            totalTime += elapsed;
            minTime = Math.min(minTime, elapsed);
            maxTime = Math.max(maxTime, elapsed);
            assertNoFailures(response);
        }

        double avgMs = (totalTime / (double) MEASURE_ITERATIONS) / 1_000_000.0;
        double minMs = minTime / 1_000_000.0;
        double maxMs = maxTime / 1_000_000.0;
        logger.info(
            "BENCHMARK [{}]: batch_enabled={}, parents={}, children_per_parent={}, search_size={}, "
                + "avg={} ms, min={} ms, max={} ms",
            index,
            batchEnabled,
            numParents,
            childrenPerParent,
            searchSize,
            String.format("%.2f", avgMs),
            String.format("%.2f", minMs),
            String.format("%.2f", maxMs)
        );
    }

    private SearchResponse executeQuery(String index, int size) {
        return client().prepareSearch(index)
            .setQuery(
                hasChildQuery("child", matchAllQuery(), ScoreMode.None)
                    .innerHit(new InnerHitBuilder().setSize(3))
            )
            .setSize(size)
            .get();
    }
}
