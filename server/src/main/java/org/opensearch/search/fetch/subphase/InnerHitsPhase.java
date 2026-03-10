/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Gets the inner hits of a document during search
 *
 * @opensearch.internal
 */
public final class InnerHitsPhase implements FetchSubPhase {

    private final FetchPhase fetchPhase;
    private final Supplier<Boolean> batchEnabledSupplier;
    private final Supplier<Integer> batchSizeSupplier;

    public InnerHitsPhase(FetchPhase fetchPhase) {
        this(fetchPhase, () -> false, () -> 1000);
    }

    public InnerHitsPhase(FetchPhase fetchPhase, Supplier<Boolean> batchEnabledSupplier, Supplier<Integer> batchSizeSupplier) {
        this.fetchPhase = fetchPhase;
        this.batchEnabledSupplier = batchEnabledSupplier;
        this.batchSizeSupplier = batchSizeSupplier;
    }

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext searchContext) {
        if (searchContext.innerHits() == null || searchContext.innerHits().getInnerHits().isEmpty()) {
            return null;
        }
        Map<String, InnerHitsContext.InnerHitSubContext> innerHits = searchContext.innerHits().getInnerHits();

        // Check if batch execution is enabled and any inner hit context supports it
        boolean hasBatchSupport = batchEnabledSupplier.get()
            && innerHits.values().stream().anyMatch(InnerHitsContext.InnerHitSubContext::supportsBatchExecution);

        if (!hasBatchSupport) {
            // Original per-hit execution path
            return new FetchSubPhaseProcessor() {
                @Override
                public void setNextReader(LeafReaderContext readerContext) {}

                @Override
                public void process(HitContext hitContext) throws IOException {
                    SearchHit hit = hitContext.hit();
                    SourceLookup rootLookup = searchContext.getRootSourceLookup(hitContext);
                    hitExecute(innerHits, hit, rootLookup);
                }
            };
        }

        // Batch execution path: accumulate hits, process in complete()
        List<SearchHit> pendingHits = new ArrayList<>();
        List<SourceLookup> pendingRootLookups = new ArrayList<>();

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {}

            @Override
            public void process(HitContext hitContext) throws IOException {
                SearchHit hit = hitContext.hit();
                SourceLookup rootLookup = searchContext.getRootSourceLookup(hitContext);
                // Process non-batch inner hits immediately
                for (Map.Entry<String, InnerHitsContext.InnerHitSubContext> entry : innerHits.entrySet()) {
                    if (!entry.getValue().supportsBatchExecution()) {
                        hitExecuteSingle(entry.getKey(), entry.getValue(), hit, rootLookup);
                    }
                }
                pendingHits.add(hit);
                pendingRootLookups.add(rootLookup);
            }

            @Override
            public void complete() throws IOException {
                if (pendingHits.isEmpty()) {
                    return;
                }
                for (Map.Entry<String, InnerHitsContext.InnerHitSubContext> entry : innerHits.entrySet()) {
                    InnerHitsContext.InnerHitSubContext innerHitsContext = entry.getValue();
                    if (!innerHitsContext.supportsBatchExecution()) {
                        continue;
                    }
                    if (pendingHits.size() > batchSizeSupplier.get()) {
                        // Fallback to sequential execution when batch size exceeded
                        for (int i = 0; i < pendingHits.size(); i++) {
                            hitExecuteSingle(entry.getKey(), innerHitsContext, pendingHits.get(i), pendingRootLookups.get(i));
                        }
                        continue;
                    }
                    Map<SearchHit, TopDocsAndMaxScore> batchResults = innerHitsContext.topDocs(pendingHits);
                    for (int i = 0; i < pendingHits.size(); i++) {
                        SearchHit hit = pendingHits.get(i);
                        SourceLookup rootLookup = pendingRootLookups.get(i);
                        TopDocsAndMaxScore topDoc = batchResults.get(hit);
                        if (topDoc == null) {
                            continue;
                        }
                        populateInnerHits(entry.getKey(), innerHitsContext, hit, rootLookup, topDoc);
                    }
                }
            }
        };
    }

    private void hitExecute(Map<String, InnerHitsContext.InnerHitSubContext> innerHits, SearchHit hit, SourceLookup rootLookup)
        throws IOException {
        for (Map.Entry<String, InnerHitsContext.InnerHitSubContext> entry : innerHits.entrySet()) {
            hitExecuteSingle(entry.getKey(), entry.getValue(), hit, rootLookup);
        }
    }

    private void hitExecuteSingle(String name, InnerHitsContext.InnerHitSubContext innerHitsContext, SearchHit hit, SourceLookup rootLookup)
        throws IOException {
        TopDocsAndMaxScore topDoc = innerHitsContext.topDocs(hit);
        populateInnerHits(name, innerHitsContext, hit, rootLookup, topDoc);
    }

    private void populateInnerHits(
        String name,
        InnerHitsContext.InnerHitSubContext innerHitsContext,
        SearchHit hit,
        SourceLookup rootLookup,
        TopDocsAndMaxScore topDoc
    ) throws IOException {
        Map<String, SearchHits> results = hit.getInnerHits();
        if (results == null) {
            hit.setInnerHits(results = new HashMap<>());
        }
        innerHitsContext.queryResult().topDocs(topDoc, innerHitsContext.sort() == null ? null : innerHitsContext.sort().formats);
        int[] docIdsToLoad = new int[topDoc.topDocs.scoreDocs.length];
        for (int j = 0; j < topDoc.topDocs.scoreDocs.length; j++) {
            docIdsToLoad[j] = topDoc.topDocs.scoreDocs[j].doc;
        }
        innerHitsContext.docIdsToLoad(docIdsToLoad, 0, docIdsToLoad.length);
        innerHitsContext.setId(hit.getId());
        innerHitsContext.setRootLookup(rootLookup);

        fetchPhase.execute(innerHitsContext, "fetch_inner_hits[" + name + "]");
        FetchSearchResult fetchResult = innerHitsContext.fetchResult();
        SearchHit[] internalHits = fetchResult.fetchResult().hits().getHits();
        for (int j = 0; j < internalHits.length; j++) {
            ScoreDoc scoreDoc = topDoc.topDocs.scoreDocs[j];
            SearchHit searchHitFields = internalHits[j];
            searchHitFields.score(scoreDoc.score);
            if (scoreDoc instanceof FieldDoc fieldDoc) {
                searchHitFields.sortValues(fieldDoc.fields, innerHitsContext.sort().formats);
            }
        }
        results.put(name, fetchResult.hits());
    }
}
