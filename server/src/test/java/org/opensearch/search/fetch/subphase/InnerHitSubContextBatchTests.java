/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch.subphase;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.search.SearchHit;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class InnerHitSubContextBatchTests extends OpenSearchTestCase {

    /**
     * Verifies that the default implementation of supportsBatchExecution() returns false.
     */
    public void testDefaultSupportsBatchExecution() {
        InnerHitsContext.InnerHitSubContext ctx = new StubInnerHitSubContext("test", mock(SearchContext.class));
        assertFalse(ctx.supportsBatchExecution());
    }

    private static SearchHit createHit(int docId) {
        return new SearchHit(docId, String.valueOf(docId), Map.of(), Map.of());
    }

    /**
     * Verifies that the default batch topDocs falls back to per-hit execution.
     */
    public void testDefaultBatchTopDocsFallsBackToPerHit() throws IOException {
        StubInnerHitSubContext ctx = new StubInnerHitSubContext("test", mock(SearchContext.class));
        List<SearchHit> hits = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            hits.add(createHit(i));
        }

        Map<SearchHit, TopDocsAndMaxScore> results = ctx.topDocs(hits);

        assertEquals(3, results.size());
        assertEquals(3, ctx.topDocsCallCount);
        for (SearchHit hit : hits) {
            TopDocsAndMaxScore result = results.get(hit);
            assertNotNull(result);
            assertEquals(1, result.topDocs.totalHits.value());
        }
    }

    /**
     * Verifies that a batch-supporting subcontext returns true for supportsBatchExecution().
     */
    public void testBatchSupportingSubContext() {
        BatchStubInnerHitSubContext ctx = new BatchStubInnerHitSubContext("test", mock(SearchContext.class));
        assertTrue(ctx.supportsBatchExecution());
    }

    /**
     * Verifies that a batch-supporting subcontext processes all hits in a single batch call.
     */
    public void testBatchTopDocsProcessesAllHits() throws IOException {
        BatchStubInnerHitSubContext ctx = new BatchStubInnerHitSubContext("test", mock(SearchContext.class));
        List<SearchHit> hits = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            hits.add(createHit(i));
        }

        Map<SearchHit, TopDocsAndMaxScore> results = ctx.topDocs(hits);

        assertEquals(5, results.size());
        assertEquals(1, ctx.batchCallCount);
        assertEquals(0, ctx.topDocsCallCount);
    }

    /**
     * Verifies that batch topDocs preserves insertion order (LinkedHashMap).
     */
    public void testBatchTopDocsPreservesOrder() throws IOException {
        BatchStubInnerHitSubContext ctx = new BatchStubInnerHitSubContext("test", mock(SearchContext.class));
        List<SearchHit> hits = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            hits.add(createHit(i));
        }

        Map<SearchHit, TopDocsAndMaxScore> results = ctx.topDocs(hits);

        List<SearchHit> resultKeys = new ArrayList<>(results.keySet());
        for (int i = 0; i < hits.size(); i++) {
            assertSame(hits.get(i), resultKeys.get(i));
        }
    }

    /**
     * Verifies that default batch topDocs handles empty list.
     */
    public void testDefaultBatchTopDocsEmptyList() throws IOException {
        StubInnerHitSubContext ctx = new StubInnerHitSubContext("test", mock(SearchContext.class));
        Map<SearchHit, TopDocsAndMaxScore> results = ctx.topDocs(List.of());
        assertTrue(results.isEmpty());
        assertEquals(0, ctx.topDocsCallCount);
    }

    private static class StubInnerHitSubContext extends InnerHitsContext.InnerHitSubContext {
        int topDocsCallCount = 0;

        StubInnerHitSubContext(String name, SearchContext context) {
            super(name, context);
        }

        @Override
        public TopDocsAndMaxScore topDocs(SearchHit hit) {
            topDocsCallCount++;
            ScoreDoc[] docs = new ScoreDoc[] { new ScoreDoc(hit.docId(), 1.0f) };
            TopDocs topDocs = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), docs);
            return new TopDocsAndMaxScore(topDocs, 1.0f);
        }
    }

    private static class BatchStubInnerHitSubContext extends InnerHitsContext.InnerHitSubContext {
        int topDocsCallCount = 0;
        int batchCallCount = 0;

        BatchStubInnerHitSubContext(String name, SearchContext context) {
            super(name, context);
        }

        @Override
        public boolean supportsBatchExecution() {
            return true;
        }

        @Override
        public TopDocsAndMaxScore topDocs(SearchHit hit) {
            topDocsCallCount++;
            ScoreDoc[] docs = new ScoreDoc[] { new ScoreDoc(hit.docId(), 1.0f) };
            TopDocs topDocs = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), docs);
            return new TopDocsAndMaxScore(topDocs, 1.0f);
        }

        @Override
        public Map<SearchHit, TopDocsAndMaxScore> topDocs(List<SearchHit> hits) {
            batchCallCount++;
            Map<SearchHit, TopDocsAndMaxScore> results = new LinkedHashMap<>();
            for (SearchHit hit : hits) {
                ScoreDoc[] docs = new ScoreDoc[] { new ScoreDoc(hit.docId(), 1.0f) };
                TopDocs topDocs = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), docs);
                results.put(hit, new TopDocsAndMaxScore(topDocs, 1.0f));
            }
            return results;
        }
    }
}
