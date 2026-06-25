/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SparkDistributedDataScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.AggregateEvaluator;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundAggregate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkAggregates;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.TimeTravel;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkScanBuilder extends BaseSparkScanBuilder
    implements SupportsPushDownV2Filters,
        SupportsPushDownRequiredColumns,
        SupportsPushDownLimit,
        SupportsPushDownAggregates {

  private static final Logger LOG = LoggerFactory.getLogger(SparkScanBuilder.class);

  private final Snapshot snapshot;
  private final String branch;
  private final TimeTravel timeTravel;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private Scan localScan;

  SparkScanBuilder(SparkSession spark, Table table, CaseInsensitiveStringMap options) {
    this(
        spark,
        table,
        table.schema(),
        table.currentSnapshot(),
        null /* no branch */,
        null /* no time travel */,
        options);
  }

  SparkScanBuilder(
      SparkSession spark,
      Table table,
      Schema schema,
      Snapshot snapshot,
      String branch,
      CaseInsensitiveStringMap options) {
    this(spark, table, schema, snapshot, branch, null /* no time travel */, options);
  }

  SparkScanBuilder(
      SparkSession spark,
      Table table,
      Schema schema,
      Snapshot snapshot,
      String branch,
      TimeTravel timeTravel,
      CaseInsensitiveStringMap options) {
    super(spark, table, schema, options);
    this.snapshot = snapshot;
    this.branch = branch;
    this.timeTravel = timeTravel;
    if (Spark3Util.containsIncrementalOptions(options)) {
      Preconditions.checkArgument(timeTravel == null, "Cannot use time travel in incremental scan");
      Pair<Long, Long> boundaries = readConf().incrementalAppendScanBoundaries();
      this.startSnapshotId = boundaries.first();
      this.endSnapshotId = boundaries.second();
    } else {
      this.startSnapshotId = null;
      this.endSnapshotId = null;
    }
    Spark3Util.validateNoLegacyTimeTravel(options);
    SparkTableUtil.validateReadBranch(spark, table, branch, options);
  }

  @Override
  public boolean pushAggregation(Aggregation aggregation) {
    if (!canPushDownAggregation(aggregation)) {
      return false;
    }

    List<BoundAggregate<?, ?>> aggregates = boundAggregates(aggregation);
    if (aggregates == null) {
      return false;
    }

    if (!metricsModeSupportsAggregatePushDown(aggregates)) {
      return false;
    }

    if (aggregation.groupByExpressions().length == 0) {
      return pushGlobalAggregation(aggregates);
    } else {
      return pushGroupedAggregation(aggregation, aggregates);
    }
  }

  private List<BoundAggregate<?, ?>> boundAggregates(Aggregation aggregation) {
    List<BoundAggregate<?, ?>> expressions =
        Lists.newArrayListWithExpectedSize(aggregation.aggregateExpressions().length);

    for (AggregateFunc aggregateFunc : aggregation.aggregateExpressions()) {
      try {
        Expression expr = SparkAggregates.convert(aggregateFunc);
        if (expr != null) {
          Expression bound = Binder.bind(projection().asStruct(), expr, caseSensitive());
          expressions.add((BoundAggregate<?, ?>) bound);
        } else {
          LOG.info(
              "Skipping aggregate pushdown: AggregateFunc {} can't be converted to iceberg expression",
              aggregateFunc);
          return null;
        }
      } catch (IllegalArgumentException e) {
        LOG.info("Skipping aggregate pushdown: Bind failed for AggregateFunc {}", aggregateFunc, e);
        return null;
      }
    }

    return expressions;
  }

  private boolean pushGlobalAggregation(List<BoundAggregate<?, ?>> aggregates) {
    AggregateEvaluator aggregateEvaluator = AggregateEvaluator.create(aggregates);

    try (CloseableIterable<FileScanTask> fileScanTasks = planFilesWithStats()) {
      for (FileScanTask task : fileScanTasks) {
        if (!task.deletes().isEmpty()) {
          LOG.info("Skipping aggregate pushdown: detected row level deletes");
          return false;
        }

        aggregateEvaluator.update(task.file());
      }
    } catch (IOException e) {
      LOG.info("Skipping aggregate pushdown: ", e);
      return false;
    }

    if (!aggregateEvaluator.allAggregatorsValid()) {
      return false;
    }

    StructType pushedAggregateSchema =
        SparkSchemaUtil.convert(new Schema(aggregateEvaluator.resultType().fields()));
    InternalRow[] pushedAggregateRows = new InternalRow[1];
    StructLike structLike = aggregateEvaluator.result();
    pushedAggregateRows[0] =
        new StructInternalRow(aggregateEvaluator.resultType()).setStruct(structLike);
    localScan = new SparkLocalScan(table(), pushedAggregateSchema, pushedAggregateRows, filters());

    return true;
  }

  /**
   * Pushes down an aggregation that groups by identity partition columns (including a bare {@code
   * SELECT DISTINCT partition_col}, which Spark plans as a group-by with no aggregate functions).
   *
   * <p>Because each data file belongs to exactly one partition, the distinct group values and any
   * supported aggregate over them can be derived from partition metadata and file statistics. One
   * result row is produced per distinct partition value combination, ordered as {@code [group by
   * columns ... aggregate columns ...]} to match the schema Spark expects from a pushed-down
   * aggregation.
   */
  private boolean pushGroupedAggregation(
      Aggregation aggregation, List<BoundAggregate<?, ?>> aggregates) {
    List<Types.NestedField> groupColumns =
        Lists.newArrayListWithExpectedSize(aggregation.groupByExpressions().length);
    for (org.apache.spark.sql.connector.expressions.Expression groupBy :
        aggregation.groupByExpressions()) {
      // canPushDownAggregation already verified each group by expression resolves to a column
      groupColumns.add(findGroupColumn(((NamedReference) groupBy).fieldNames()[0]));
    }

    Types.StructType groupType = Types.StructType.of(groupColumns);
    StructLikeWrapper wrapper = StructLikeWrapper.forType(groupType);
    Map<StructLikeWrapper, AggregateEvaluator> evaluators = Maps.newLinkedHashMap();

    try (CloseableIterable<FileScanTask> fileScanTasks = planFilesWithStats()) {
      for (FileScanTask task : fileScanTasks) {
        if (!task.deletes().isEmpty()) {
          LOG.info("Skipping aggregate pushdown: detected row level deletes");
          return false;
        }

        StructLike groupKey = groupKey(task, groupColumns);
        if (groupKey == null) {
          LOG.info(
              "Skipping aggregate pushdown: group by column is not an identity partition column in spec {}",
              task.spec().specId());
          return false;
        }

        evaluators
            .computeIfAbsent(
                wrapper.copyFor(groupKey), key -> AggregateEvaluator.create(aggregates))
            .update(task.file());
      }
    } catch (IOException e) {
      LOG.info("Skipping aggregate pushdown: ", e);
      return false;
    }

    Types.StructType resultType = groupedResultType(groupColumns, aggregates);
    StructType pushedAggregateSchema = SparkSchemaUtil.convert(new Schema(resultType.fields()));
    InternalRow[] pushedAggregateRows = new InternalRow[evaluators.size()];
    int rowIndex = 0;
    for (Map.Entry<StructLikeWrapper, AggregateEvaluator> entry : evaluators.entrySet()) {
      AggregateEvaluator aggregateEvaluator = entry.getValue();
      if (!aggregateEvaluator.allAggregatorsValid()) {
        return false;
      }

      StructLike row = new JoinedStructLike(entry.getKey().get(), aggregateEvaluator.result());
      pushedAggregateRows[rowIndex] = new StructInternalRow(resultType).setStruct(row);
      rowIndex += 1;
    }

    localScan = new SparkLocalScan(table(), pushedAggregateSchema, pushedAggregateRows, filters());

    return true;
  }

  private boolean canPushDownAggregation(Aggregation aggregation) {
    if (!isMainTable()) {
      return false;
    }

    if (!readConf().aggregatePushDownEnabled()) {
      return false;
    }

    // Group by push down is only supported when every grouping key is a top-level column
    // partitioned by the identity transform. In that case the distinct group values (and any
    // min/max/count over them) can be derived from partition metadata rather than scanning data.
    for (org.apache.spark.sql.connector.expressions.Expression groupBy :
        aggregation.groupByExpressions()) {
      if (!(groupBy instanceof NamedReference)) {
        LOG.info("Skipping aggregate pushdown: group by expression {} is not a column", groupBy);
        return false;
      }

      NamedReference reference = (NamedReference) groupBy;
      if (reference.fieldNames().length != 1) {
        LOG.info(
            "Skipping aggregate pushdown: group by on nested column {} is not supported",
            String.join(".", reference.fieldNames()));
        return false;
      }

      Types.NestedField column = findGroupColumn(reference.fieldNames()[0]);
      if (column == null || !isIdentityPartitionColumn(column.fieldId())) {
        LOG.info(
            "Skipping aggregate pushdown: group by column {} is not an identity partition column",
            reference.fieldNames()[0]);
        return false;
      }
    }

    return true;
  }

  private Types.NestedField findGroupColumn(String name) {
    return caseSensitive()
        ? table().schema().findField(name)
        : table().schema().caseInsensitiveFindField(name);
  }

  private boolean isIdentityPartitionColumn(int sourceId) {
    for (PartitionSpec spec : table().specs().values()) {
      if (identityPartitionPosition(spec, sourceId) >= 0) {
        return true;
      }
    }

    return false;
  }

  private int identityPartitionPosition(PartitionSpec spec, int sourceId) {
    List<PartitionField> fields = spec.fields();
    for (int position = 0; position < fields.size(); position++) {
      PartitionField field = fields.get(position);
      if (field.transform().isIdentity() && field.sourceId() == sourceId) {
        return position;
      }
    }

    return -1;
  }

  private StructLike groupKey(FileScanTask task, List<Types.NestedField> groupColumns) {
    PartitionSpec spec = task.spec();
    StructLike partition = task.file().partition();
    Object[] values = new Object[groupColumns.size()];
    for (int i = 0; i < groupColumns.size(); i++) {
      Types.NestedField column = groupColumns.get(i);
      int position = identityPartitionPosition(spec, column.fieldId());
      if (position < 0) {
        return null;
      }

      values[i] = partition.get(position, column.type().typeId().javaClass());
    }

    return new ArrayStructLike(values);
  }

  private Types.StructType groupedResultType(
      List<Types.NestedField> groupColumns, List<BoundAggregate<?, ?>> aggregates) {
    List<Types.NestedField> fields =
        Lists.newArrayListWithExpectedSize(groupColumns.size() + aggregates.size());
    int fieldId = 0;
    for (Types.NestedField groupColumn : groupColumns) {
      fields.add(Types.NestedField.optional(fieldId, groupColumn.name(), groupColumn.type()));
      fieldId += 1;
    }

    for (BoundAggregate<?, ?> aggregate : aggregates) {
      fields.add(Types.NestedField.optional(fieldId, aggregate.describe(), aggregate.type()));
      fieldId += 1;
    }

    return Types.StructType.of(fields);
  }

  private boolean metricsModeSupportsAggregatePushDown(List<BoundAggregate<?, ?>> aggregates) {
    MetricsConfig config = MetricsConfig.forTable(table());
    for (BoundAggregate aggregate : aggregates) {
      String colName = aggregate.columnName();
      if (!colName.equals("*")) {
        MetricsModes.MetricsMode mode = config.columnMode(colName);
        if (mode instanceof MetricsModes.None) {
          LOG.info("Skipping aggregate pushdown: No metrics for column {}", colName);
          return false;
        } else if (mode instanceof MetricsModes.Counts) {
          if (aggregate.op() == Expression.Operation.MAX
              || aggregate.op() == Expression.Operation.MIN) {
            LOG.info(
                "Skipping aggregate pushdown: Cannot produce min or max from count for column {}",
                colName);
            return false;
          }
        } else if (aggregate.type().typeId() == Type.TypeID.STRING
            || aggregate.type().typeId() == Type.TypeID.BINARY) {
          // lower_bounds and upper_bounds may have been truncated before, so disable push down
          // regardless of the current mode
          if (aggregate.op() == Expression.Operation.MAX
              || aggregate.op() == Expression.Operation.MIN) {
            LOG.info(
                "Skipping aggregate pushdown: Cannot produce min or max from truncated values for column {}",
                colName);
            return false;
          }
        }
      }
    }

    return true;
  }

  @Override
  public Scan build() {
    if (localScan != null) {
      return localScan;
    } else if (startSnapshotId != null) {
      return buildIncrementalAppendScan();
    } else {
      return buildBatchScan();
    }
  }

  private Scan buildBatchScan() {
    Schema projection = projectionWithMetadataColumns();
    return new SparkBatchQueryScan(
        spark(),
        table(),
        schema(),
        snapshot,
        branch,
        buildIcebergBatchScan(projection, false /* use residuals */, false /* no stats */),
        readConf(),
        projection,
        filters(),
        metricsReporter()::scanReport);
  }

  private Scan buildIncrementalAppendScan() {
    Schema projection = projectionWithMetadataColumns();
    return new SparkIncrementalAppendScan(
        spark(),
        table(),
        startSnapshotId,
        endSnapshotId,
        buildIcebergIncrementalAppendScan(projection, false /* no stats */),
        readConf(),
        projection,
        filters(),
        metricsReporter()::scanReport);
  }

  public Scan buildCopyOnWriteScan() {
    Schema projection = projectionWithMetadataColumns();
    return new SparkCopyOnWriteScan(
        spark(),
        table(),
        schema(),
        snapshot,
        branch,
        buildIcebergBatchScan(projection, true /* ignore residuals */, false /* no stats */),
        readConf(),
        projection,
        filters(),
        metricsReporter()::scanReport);
  }

  private CloseableIterable<FileScanTask> planFilesWithStats() {
    Schema projection = projectionWithMetadataColumns();
    org.apache.iceberg.Scan<?, ?, ?> scan = buildIcebergScanWithStats(projection);
    if (scan != null) {
      return CloseableIterable.transform(scan.planFiles(), ScanTask::asFileScanTask);
    } else {
      return CloseableIterable.empty();
    }
  }

  private org.apache.iceberg.Scan<?, ?, ?> buildIcebergScanWithStats(Schema projection) {
    if (startSnapshotId != null) {
      return buildIcebergIncrementalAppendScan(projection, true /* with stats */);
    } else {
      return buildIcebergBatchScan(projection, false /* use residuals */, true /* with stats */);
    }
  }

  private IncrementalAppendScan buildIcebergIncrementalAppendScan(
      Schema projection, boolean withStats) {
    IncrementalAppendScan scan =
        table()
            .newIncrementalAppendScan()
            .fromSnapshotExclusive(startSnapshotId)
            .caseSensitive(caseSensitive())
            .filter(filter())
            .project(projection)
            .metricsReporter(metricsReporter());

    if (withStats) {
      scan = scan.includeColumnStats();
    }

    if (endSnapshotId != null) {
      scan = scan.toSnapshot(endSnapshotId);
    }

    return configureSplitPlanning(scan);
  }

  private BatchScan buildIcebergBatchScan(
      Schema projection, boolean ignoreResiduals, boolean withStats) {
    if (shouldPinSnapshot() && snapshot == null) {
      return null;
    }

    BatchScan scan =
        newIcebergBatchScan()
            .caseSensitive(caseSensitive())
            .filter(filter())
            .project(projection)
            .metricsReporter(metricsReporter());

    if (shouldPinSnapshot() || timeTravel != null) {
      scan = scan.useSnapshot(snapshot.snapshotId());
    }

    Preconditions.checkState(
        Objects.equals(snapshot, scan.snapshot()),
        "Failed to enforce scan consistency: resolved Spark table snapshot (%s) vs scan snapshot (%s)",
        snapshot,
        scan.snapshot());

    if (ignoreResiduals) {
      scan = scan.ignoreResiduals();
    }

    if (withStats) {
      scan = scan.includeColumnStats();
    }

    return configureSplitPlanning(scan);
  }

  private BatchScan newIcebergBatchScan() {
    if (readConf().distributedPlanningEnabled()) {
      return new SparkDistributedDataScan(spark(), table(), readConf());
    } else {
      return table().newBatchScan();
    }
  }

  private boolean shouldPinSnapshot() {
    return isMainTable() || isMetadataTableWithTimeTravel();
  }

  private boolean isMainTable() {
    return table() instanceof BaseTable;
  }

  private boolean isMetadataTableWithTimeTravel() {
    if (table() instanceof BaseMetadataTable metadataTable) {
      return metadataTable.supportsTimeTravel();
    } else {
      return false;
    }
  }

  /** A read-only {@link StructLike} backed by an array of values. */
  private static class ArrayStructLike implements StructLike {
    private final Object[] values;

    private ArrayStructLike(Object[] values) {
      this.values = values;
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("ArrayStructLike is read-only");
    }
  }

  /** A read-only {@link StructLike} that concatenates the fields of two structs. */
  private static class JoinedStructLike implements StructLike {
    private final StructLike left;
    private final StructLike right;

    private JoinedStructLike(StructLike left, StructLike right) {
      this.left = left;
      this.right = right;
    }

    @Override
    public int size() {
      return left.size() + right.size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      if (pos < left.size()) {
        return left.get(pos, javaClass);
      } else {
        return right.get(pos - left.size(), javaClass);
      }
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("JoinedStructLike is read-only");
    }
  }
}
