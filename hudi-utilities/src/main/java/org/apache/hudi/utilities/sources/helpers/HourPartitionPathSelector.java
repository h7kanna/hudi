/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.utilities.sources.helpers.DFSPathSelector.Config.ROOT_INPUT_PATH_PROP;
import static org.apache.hudi.utilities.sources.helpers.HourPartitionPathSelector.Config.PARTITION_FORMAT;
import static org.apache.hudi.utilities.sources.helpers.HourPartitionPathSelector.Config.DEFAULT_PARTITION_FORMAT;
import static org.apache.hudi.utilities.sources.helpers.HourPartitionPathSelector.Config.HOUR_PARTITION_DEPTH;
import static org.apache.hudi.utilities.sources.helpers.HourPartitionPathSelector.Config.DEFAULT_HOUR_PARTITION_DEPTH;
import static org.apache.hudi.utilities.sources.helpers.HourPartitionPathSelector.Config.PARTITION_DELAY;
import static org.apache.hudi.utilities.sources.helpers.HourPartitionPathSelector.Config.DEFAULT_PARTITION_DELAY;
import static org.apache.hudi.utilities.sources.helpers.HourPartitionPathSelector.Config.PARTITIONS_LIST_PARALLELISM;
import static org.apache.hudi.utilities.sources.helpers.HourPartitionPathSelector.Config.DEFAULT_PARTITIONS_LIST_PARALLELISM;

/**
 * Custom dfs path selector used to list current hour partition.
 *
 * <p>This is useful for workloads where there are multiple partition fields and only recent
 * partitions are affected by new writes. Especially if the data sits in S3, listing all historical
 * data can be time expensive and unnecessary for the above type of workload.
 *
 * <p>The partition is expected to be of the format '%d/%d/%02d/%02d' or
 * 'year=%d/month=%d/day=%02d/hour=%02d'. The partition can be at any level. For ex. the partition path can be of the
 * form `<basepath>/<partition-field1>/<date-based-partition>/<partition-field3>/` or
 * `<basepath>/<<date-based-partition>/`.
 *
 * <p>The partition format can be configured via this property
 * hoodie.deltastreamer.source.dfs.hourpartitioned.partition.format
 */
public class HourPartitionPathSelector extends DFSPathSelector {

  private static final Logger LOG = LogManager.getLogger(HourPartitionPathSelector.class);

  private final String partitionFormat;
  private final int partitionDepth;
  private final long partitionDelayInSeconds;
  private final int partitionsListParallelism;
  private Clock clock = Clock.systemUTC();

  /** Configs supported. */
  public static class Config {
    public static final String PARTITION_FORMAT =
        "hoodie.deltastreamer.source.dfs.hourpartitioned.partition.format";
    public static final String DEFAULT_PARTITION_FORMAT = "%d/%d/%02d/%02d";

    public static final String HOUR_PARTITION_DEPTH =
        "hoodie.deltastreamer.source.dfs.hourpartitioned.selector.depth";
    public static final int DEFAULT_HOUR_PARTITION_DEPTH = 0; // Implies no (hour) partition

    public static final String PARTITION_DELAY =
        "hoodie.deltastreamer.source.dfs.hourpartitioned.selector.partition.delay";
    public static final int DEFAULT_PARTITION_DELAY = 300;

    public static final String PARTITIONS_LIST_PARALLELISM =
        "hoodie.deltastreamer.source.dfs.hourpartitioned.selector.parallelism";
    public static final int DEFAULT_PARTITIONS_LIST_PARALLELISM = 20;
  }

  public HourPartitionPathSelector(TypedProperties props, Configuration hadoopConf) {
    super(props, hadoopConf);
    /*
     * partitionDepth = 0 is same as basepath and there is no partition. In which case
     * this path selector would be a no-op and lists all paths under the table basepath.
     */
    partitionFormat = props.getString(PARTITION_FORMAT, DEFAULT_PARTITION_FORMAT);
    partitionDepth = props.getInteger(HOUR_PARTITION_DEPTH, DEFAULT_HOUR_PARTITION_DEPTH);
    partitionDelayInSeconds = props.getLong(PARTITION_DELAY, DEFAULT_PARTITION_DELAY);
    partitionsListParallelism = props.getInteger(PARTITIONS_LIST_PARALLELISM, DEFAULT_PARTITIONS_LIST_PARALLELISM);
  }

  public HourPartitionPathSelector(TypedProperties props, Configuration hadoopConf, LocalDateTime currentTime) {
    this(props,hadoopConf);
    this.clock = Clock.fixed(currentTime.toInstant(ZoneOffset.UTC), ZoneId.systemDefault());
  }

  @Override
  public Pair<Option<String>, String> getNextFilePathsAndMaxModificationTime(JavaSparkContext sparkContext,
                                                                             Option<String> lastCheckpointStr,
                                                                             long sourceLimit) {
    // If delay is not specified the default delay is assumed.
    LocalDateTime currentDateTime = LocalDateTime.now(ZoneOffset.UTC).minus(partitionDelayInSeconds, ChronoUnit.SECONDS);
    String currentPartition = String.format(partitionFormat,
            currentDateTime.getYear(), currentDateTime.getMonthValue(), currentDateTime.getDayOfMonth(), currentDateTime.getHour());

    // obtain all eligible files under root folder.
    LOG.info(
        "Root path => "
            + props.getString(ROOT_INPUT_PATH_PROP)
            + " source limit => "
            + sourceLimit
            + " depth of hour partition => "
            + partitionDepth
            + " partition delay in seconds => "
            + partitionDelayInSeconds
            + " current date time => "
            + currentDateTime
            + " current partition => "
            + currentPartition);

    long lastCheckpointTime = lastCheckpointStr.map(Long::parseLong).orElse(Long.MIN_VALUE);
    HoodieSparkEngineContext context = new HoodieSparkEngineContext(sparkContext);
    SerializableConfiguration serializedConf = new SerializableConfiguration(fs.getConf());
    List<String> currentHourPartitionPaths = getCurrentHourPartitionPaths(context, fs, props.getString(ROOT_INPUT_PATH_PROP), currentPartition);

    List<FileStatus> eligibleFiles = context.flatMap(currentHourPartitionPaths,
        path -> {
          FileSystem fs = new Path(path).getFileSystem(serializedConf.get());
          return listEligibleFiles(fs, new Path(path), lastCheckpointTime).stream();
        }, partitionsListParallelism);
    // sort them by modification time ascending.
    List<FileStatus> sortedEligibleFiles = eligibleFiles.stream()
        .sorted(Comparator.comparingLong(FileStatus::getModificationTime)).collect(Collectors.toList());

    // Filter based on checkpoint & input size, if needed
    long currentBytes = 0;
    long newCheckpointTime = lastCheckpointTime;
    List<FileStatus> filteredFiles = new ArrayList<>();
    for (FileStatus f : sortedEligibleFiles) {
      if (currentBytes + f.getLen() >= sourceLimit && f.getModificationTime() > newCheckpointTime) {
        // we have enough data, we are done
        // Also, we've read up to a file with a newer modification time
        // so that some files with the same modification time won't be skipped in next read
        break;
      }

      newCheckpointTime = f.getModificationTime();
      currentBytes += f.getLen();
      filteredFiles.add(f);
    }

    // no data to read
    if (filteredFiles.isEmpty()) {
      return new ImmutablePair<>(Option.empty(), String.valueOf(newCheckpointTime));
    }

    // read the files out.
    String pathStr = filteredFiles.stream().map(f -> f.getPath().toString()).collect(Collectors.joining(","));

    LOG.info(
            "Selected files => "
                    + pathStr
                    + " last checkpoint => "
                    + lastCheckpointTime
                    + " current date time => "
                    + currentDateTime
                    + " current partition => "
                    + currentPartition);

    return new ImmutablePair<>(Option.ofNullable(pathStr), String.valueOf(newCheckpointTime));
  }

  /**
   * Get all files under current hour partition.
   * Parallelizes listing by leveraging HoodieSparkEngineContext's methods.
   */
  public List<String> getCurrentHourPartitionPaths(HoodieSparkEngineContext context, FileSystem fs, String rootPath, String currentPartition) {
    List<String> partitionPaths = new ArrayList<>();
    // get all partition paths from the current hour
    partitionPaths.add(rootPath + "/" + currentPartition);
    if (partitionDepth <= 0) {
      return partitionPaths;
    }
    SerializableConfiguration serializedConf = new SerializableConfiguration(fs.getConf());
    for (int i = 0; i < partitionDepth; i++) {
      partitionPaths = context.flatMap(partitionPaths, path -> {
        Path subDir = new Path(path);
        FileSystem fileSystem = subDir.getFileSystem(serializedConf.get());
        // skip files/dirs whose names start with (_, ., etc)
        FileStatus[] statuses = fileSystem.listStatus(subDir,
            file -> IGNORE_FILEPREFIX_LIST.stream().noneMatch(pfx -> file.getName().startsWith(pfx)));
        List<String> res = new ArrayList<>();
        for (FileStatus status : statuses) {
          res.add(status.getPath().toString());
        }
        return res.stream();
      }, partitionsListParallelism);
    }

    return partitionPaths;
  }
}
