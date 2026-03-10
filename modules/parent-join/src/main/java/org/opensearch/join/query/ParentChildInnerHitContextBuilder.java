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

package org.opensearch.join.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.search.MaxScoreCollector;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.join.mapper.ParentIdFieldMapper;
import org.opensearch.join.mapper.ParentJoinFieldMapper;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.subphase.InnerHitsContext;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.search.fetch.subphase.InnerHitsContext.intersect;

class ParentChildInnerHitContextBuilder extends InnerHitContextBuilder {
    private final String typeName;
    private final boolean fetchChildInnerHits;

    ParentChildInnerHitContextBuilder(
        String typeName,
        boolean fetchChildInnerHits,
        QueryBuilder query,
        InnerHitBuilder innerHitBuilder,
        Map<String, InnerHitContextBuilder> children
    ) {
        super(query, innerHitBuilder, children);
        this.typeName = typeName;
        this.fetchChildInnerHits = fetchChildInnerHits;
    }

    @Override
    protected void doBuild(SearchContext context, InnerHitsContext innerHitsContext) throws IOException {
        QueryShardContext queryShardContext = context.getQueryShardContext();
        ParentJoinFieldMapper joinFieldMapper = ParentJoinFieldMapper.getMapper(context.mapperService());
        if (joinFieldMapper != null) {
            String name = innerHitBuilder.getName() != null ? innerHitBuilder.getName() : typeName;
            JoinFieldInnerHitSubContext joinFieldInnerHits = new JoinFieldInnerHitSubContext(
                name,
                context,
                typeName,
                fetchChildInnerHits,
                joinFieldMapper
            );
            setupInnerHitsContext(queryShardContext, joinFieldInnerHits);
            innerHitsContext.addInnerHitDefinition(joinFieldInnerHits);
        } else {
            if (innerHitBuilder.isIgnoreUnmapped() == false) {
                throw new IllegalStateException("no join field has been configured");
            }
        }
    }

    static final class JoinFieldInnerHitSubContext extends InnerHitsContext.InnerHitSubContext {
        private final String typeName;
        private final boolean fetchChildInnerHits;
        private final ParentJoinFieldMapper joinFieldMapper;

        JoinFieldInnerHitSubContext(
            String name,
            SearchContext context,
            String typeName,
            boolean fetchChildInnerHits,
            ParentJoinFieldMapper joinFieldMapper
        ) {
            super(name, context);
            this.typeName = typeName;
            this.fetchChildInnerHits = fetchChildInnerHits;
            this.joinFieldMapper = joinFieldMapper;
        }

        @Override
        public TopDocsAndMaxScore topDocs(SearchHit hit) throws IOException {
            Weight innerHitQueryWeight = getInnerHitQueryWeight();
            String joinName = getSortedDocValue(joinFieldMapper.name(), context, hit.docId());
            if (joinName == null) {
                return new TopDocsAndMaxScore(Lucene.EMPTY_TOP_DOCS, Float.NaN);
            }

            QueryShardContext qsc = context.getQueryShardContext();
            ParentIdFieldMapper parentIdFieldMapper = joinFieldMapper.getParentIdFieldMapper(typeName, fetchChildInnerHits == false);
            if (parentIdFieldMapper == null) {
                return new TopDocsAndMaxScore(Lucene.EMPTY_TOP_DOCS, Float.NaN);
            }

            Query q;
            if (fetchChildInnerHits) {
                Query hitQuery = parentIdFieldMapper.fieldType().termQuery(hit.getId(), qsc);
                q = new BooleanQuery.Builder()
                    // Only include child documents that have the current hit as parent:
                    .add(hitQuery, BooleanClause.Occur.FILTER)
                    // and only include child documents of a single relation:
                    .add(joinFieldMapper.fieldType().termQuery(typeName, qsc), BooleanClause.Occur.FILTER)
                    .build();
            } else {
                String parentId = getSortedDocValue(parentIdFieldMapper.name(), context, hit.docId());
                if (parentId == null) {
                    return new TopDocsAndMaxScore(Lucene.EMPTY_TOP_DOCS, Float.NaN);
                }
                q = context.mapperService().fieldType(IdFieldMapper.NAME).termQuery(parentId, qsc);
            }

            Weight weight = context.searcher().createWeight(context.searcher().rewrite(q), ScoreMode.COMPLETE_NO_SCORES, 1f);
            if (size() == 0) {
                TotalHitCountCollector totalHitCountCollector = new TotalHitCountCollector();
                for (LeafReaderContext ctx : context.searcher().getIndexReader().leaves()) {
                    intersect(weight, innerHitQueryWeight, totalHitCountCollector, ctx);
                }
                return new TopDocsAndMaxScore(
                    new TopDocs(new TotalHits(totalHitCountCollector.getTotalHits(), TotalHits.Relation.EQUAL_TO), Lucene.EMPTY_SCORE_DOCS),
                    Float.NaN
                );
            } else {
                int topN = Math.min(from() + size(), context.searcher().getIndexReader().maxDoc());
                TopDocsCollector<?> topDocsCollector;
                MaxScoreCollector maxScoreCollector = null;
                if (sort() != null) {
                    topDocsCollector = new TopFieldCollectorManager(sort().sort, topN, null, Integer.MAX_VALUE, false).newCollector();
                    if (trackScores()) {
                        maxScoreCollector = new MaxScoreCollector();
                    }
                } else {
                    topDocsCollector = new TopScoreDocCollectorManager(topN, null, Integer.MAX_VALUE).newCollector();
                    maxScoreCollector = new MaxScoreCollector();
                }
                for (LeafReaderContext ctx : context.searcher().getIndexReader().leaves()) {
                    intersect(weight, innerHitQueryWeight, MultiCollector.wrap(topDocsCollector, maxScoreCollector), ctx);
                }
                TopDocs topDocs = topDocsCollector.topDocs(from(), size());
                float maxScore = Float.NaN;
                if (maxScoreCollector != null) {
                    maxScore = maxScoreCollector.getMaxScore();
                }
                return new TopDocsAndMaxScore(topDocs, maxScore);
            }
        }

        @Override
        public boolean supportsBatchExecution() {
            return fetchChildInnerHits;
        }

        @Override
        public Map<SearchHit, TopDocsAndMaxScore> topDocs(List<SearchHit> hits) throws IOException {
            if (!fetchChildInnerHits) {
                Map<SearchHit, TopDocsAndMaxScore> results = new LinkedHashMap<>();
                for (SearchHit hit : hits) {
                    results.put(hit, topDocs(hit));
                }
                return results;
            }

            Weight innerHitQueryWeight = getInnerHitQueryWeight();
            QueryShardContext qsc = context.getQueryShardContext();
            ParentIdFieldMapper parentIdFieldMapper = joinFieldMapper.getParentIdFieldMapper(typeName, false);

            Map<SearchHit, TopDocsAndMaxScore> results = new LinkedHashMap<>();
            if (parentIdFieldMapper == null) {
                for (SearchHit hit : hits) {
                    results.put(hit, new TopDocsAndMaxScore(Lucene.EMPTY_TOP_DOCS, Float.NaN));
                }
                return results;
            }

            // Filter valid hits and build parent ID -> hit mapping
            Map<String, SearchHit> parentIdToHit = new LinkedHashMap<>();
            for (SearchHit hit : hits) {
                String joinName = getSortedDocValue(joinFieldMapper.name(), context, hit.docId());
                if (joinName == null) {
                    results.put(hit, new TopDocsAndMaxScore(Lucene.EMPTY_TOP_DOCS, Float.NaN));
                } else {
                    parentIdToHit.put(hit.getId(), hit);
                }
            }

            if (parentIdToHit.isEmpty()) {
                return results;
            }

            // Build a single batch query: parentIdField IN (id1, id2, ...) AND joinField == typeName
            Query parentIdFilter = parentIdFieldMapper.fieldType().termsQuery(new ArrayList<>(parentIdToHit.keySet()), qsc);
            Query batchQuery = new BooleanQuery.Builder()
                .add(parentIdFilter, BooleanClause.Occur.FILTER)
                .add(joinFieldMapper.fieldType().termQuery(typeName, qsc), BooleanClause.Occur.FILTER)
                .build();

            Weight batchWeight = context.searcher()
                .createWeight(context.searcher().rewrite(batchQuery), ScoreMode.COMPLETE_NO_SCORES, 1f);

            String parentIdFieldName = parentIdFieldMapper.fieldType().name();

            if (size() == 0) {
                // Count-only mode
                Map<String, Integer> parentIdToCounts = new HashMap<>();
                for (LeafReaderContext ctx : context.searcher().getIndexReader().leaves()) {
                    collectCountsForBatch(batchWeight, innerHitQueryWeight, ctx, parentIdFieldName, parentIdToCounts, parentIdToHit.keySet());
                }
                for (Map.Entry<String, SearchHit> entry : parentIdToHit.entrySet()) {
                    int count = parentIdToCounts.getOrDefault(entry.getKey(), 0);
                    results.put(
                        entry.getValue(),
                        new TopDocsAndMaxScore(
                            new TopDocs(new TotalHits(count, TotalHits.Relation.EQUAL_TO), Lucene.EMPTY_SCORE_DOCS),
                            Float.NaN
                        )
                    );
                }
            } else {
                // Create per-parent collectors
                int topN = Math.min(from() + size(), context.searcher().getIndexReader().maxDoc());
                Map<String, TopDocsCollector<?>> parentIdToCollector = new HashMap<>();
                Map<String, MaxScoreCollector> parentIdToMaxScore = new HashMap<>();

                for (String parentId : parentIdToHit.keySet()) {
                    TopDocsCollector<?> topDocsCollector;
                    MaxScoreCollector maxScoreCollector = null;
                    if (sort() != null) {
                        topDocsCollector = new TopFieldCollectorManager(sort().sort, topN, null, Integer.MAX_VALUE, false).newCollector();
                        if (trackScores()) {
                            maxScoreCollector = new MaxScoreCollector();
                        }
                    } else {
                        topDocsCollector = new TopScoreDocCollectorManager(topN, null, Integer.MAX_VALUE).newCollector();
                        maxScoreCollector = new MaxScoreCollector();
                    }
                    parentIdToCollector.put(parentId, topDocsCollector);
                    if (maxScoreCollector != null) {
                        parentIdToMaxScore.put(parentId, maxScoreCollector);
                    }
                }

                // Single scan: dispatch each child doc to the appropriate parent's collector
                for (LeafReaderContext ctx : context.searcher().getIndexReader().leaves()) {
                    collectAndDispatchForBatch(
                        batchWeight,
                        innerHitQueryWeight,
                        ctx,
                        parentIdFieldName,
                        parentIdToCollector,
                        parentIdToMaxScore
                    );
                }

                // Build results from collectors
                for (Map.Entry<String, SearchHit> entry : parentIdToHit.entrySet()) {
                    String parentId = entry.getKey();
                    SearchHit hit = entry.getValue();
                    TopDocsCollector<?> collector = parentIdToCollector.get(parentId);
                    TopDocs topDocs = collector.topDocs(from(), size());
                    float maxScore = Float.NaN;
                    MaxScoreCollector maxScoreCollector = parentIdToMaxScore.get(parentId);
                    if (maxScoreCollector != null) {
                        maxScore = maxScoreCollector.getMaxScore();
                    }
                    results.put(hit, new TopDocsAndMaxScore(topDocs, maxScore));
                }
            }
            return results;
        }

        private void collectCountsForBatch(
            Weight batchWeight,
            Weight innerHitQueryWeight,
            LeafReaderContext ctx,
            String parentIdFieldName,
            Map<String, Integer> parentIdToCounts,
            java.util.Set<String> allParentIds
        ) throws IOException {
            SortedDocValues parentIdValues = ctx.reader().getSortedDocValues(parentIdFieldName);
            if (parentIdValues == null) {
                return;
            }
            Map<Integer, String> ordToParentId = new HashMap<>();
            for (String parentId : allParentIds) {
                int ord = parentIdValues.lookupTerm(new BytesRef(parentId));
                if (ord >= 0) {
                    ordToParentId.put(ord, parentId);
                }
            }
            if (ordToParentId.isEmpty()) {
                return;
            }
            intersect(batchWeight, innerHitQueryWeight, new Collector() {
                @Override
                public org.apache.lucene.search.LeafCollector getLeafCollector(LeafReaderContext leafCtx) {
                    return new org.apache.lucene.search.LeafCollector() {
                        @Override
                        public void setScorer(org.apache.lucene.search.Scorable scorer) {}

                        @Override
                        public void collect(int doc) throws IOException {
                            if (parentIdValues.advanceExact(doc)) {
                                String parentId = ordToParentId.get(parentIdValues.ordValue());
                                if (parentId != null) {
                                    parentIdToCounts.merge(parentId, 1, Integer::sum);
                                }
                            }
                        }
                    };
                }

                @Override
                public ScoreMode scoreMode() {
                    return ScoreMode.COMPLETE_NO_SCORES;
                }
            }, ctx);
        }

        private void collectAndDispatchForBatch(
            Weight batchWeight,
            Weight innerHitQueryWeight,
            LeafReaderContext ctx,
            String parentIdFieldName,
            Map<String, TopDocsCollector<?>> parentIdToCollector,
            Map<String, MaxScoreCollector> parentIdToMaxScore
        ) throws IOException {
            SortedDocValues parentIdValues = ctx.reader().getSortedDocValues(parentIdFieldName);
            if (parentIdValues == null) {
                return;
            }

            // Pre-resolve parent IDs to ordinals for fast lookup
            Map<Integer, String> ordToParentId = new HashMap<>();
            for (String parentId : parentIdToCollector.keySet()) {
                int ord = parentIdValues.lookupTerm(new BytesRef(parentId));
                if (ord >= 0) {
                    ordToParentId.put(ord, parentId);
                }
            }
            if (ordToParentId.isEmpty()) {
                return;
            }

            // Pre-create all leaf collectors for this segment
            Map<String, org.apache.lucene.search.LeafCollector> leafCollectors = new HashMap<>();

            intersect(batchWeight, innerHitQueryWeight, new Collector() {
                @Override
                public org.apache.lucene.search.LeafCollector getLeafCollector(LeafReaderContext leafCtx) {
                    // Reset leaf collectors for new segment
                    leafCollectors.clear();
                    return new org.apache.lucene.search.LeafCollector() {
                        org.apache.lucene.search.Scorable currentScorer;

                        @Override
                        public void setScorer(org.apache.lucene.search.Scorable scorer) {
                            this.currentScorer = scorer;
                        }

                        @Override
                        public void collect(int doc) throws IOException {
                            if (parentIdValues.advanceExact(doc)) {
                                int ord = parentIdValues.ordValue();
                                String parentId = ordToParentId.get(ord);
                                if (parentId == null) {
                                    return;
                                }
                                org.apache.lucene.search.LeafCollector lc = leafCollectors.get(parentId);
                                if (lc == null) {
                                    Collector wrapped = MultiCollector.wrap(
                                        parentIdToCollector.get(parentId),
                                        parentIdToMaxScore.get(parentId)
                                    );
                                    lc = wrapped.getLeafCollector(leafCtx);
                                    lc.setScorer(currentScorer);
                                    leafCollectors.put(parentId, lc);
                                }
                                lc.collect(doc);
                            }
                        }
                    };
                }

                @Override
                public ScoreMode scoreMode() {
                    return sort() != null && !trackScores() ? ScoreMode.COMPLETE_NO_SCORES : ScoreMode.COMPLETE;
                }
            }, ctx);
        }

        private String getSortedDocValue(String field, SearchContext context, int docId) {
            try {
                List<LeafReaderContext> ctxs = context.searcher().getIndexReader().leaves();
                LeafReaderContext ctx = ctxs.get(ReaderUtil.subIndex(docId, ctxs));
                SortedDocValues docValues = ctx.reader().getSortedDocValues(field);
                int segmentDocId = docId - ctx.docBase;
                if (docValues == null || docValues.advanceExact(segmentDocId) == false) {
                    return null;
                }
                int ord = docValues.ordValue();
                BytesRef joinName = docValues.lookupOrd(ord);
                return joinName.utf8ToString();
            } catch (IOException e) {
                throw ExceptionsHelper.convertToOpenSearchException(e);
            }
        }

    }

}
