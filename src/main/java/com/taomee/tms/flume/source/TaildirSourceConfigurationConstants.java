/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.taomee.tms.flume.source;

public class TaildirSourceConfigurationConstants {
  /** Mapping for tailing file groups. */
  public static final String FILE_GROUPS = "filegroups";
  public static final String FILE_GROUPS_PREFIX = FILE_GROUPS + ".";

  /** Mapping for putting headers to events grouped by file groups. */
  public static final String HEADERS_PREFIX = "headers.";

  /** Path of position file. */
  public static final String POSITION_FILE = "positionFile";
  public static final String DEFAULT_POSITION_FILE = "/.flume/taildir_position.json";

  /** What size to batch with before sending to ChannelProcessor. */
  public static final String BATCH_SIZE = "batchSize";
  public static final int DEFAULT_BATCH_SIZE = 100;

  /** Whether to skip the position to EOF in the case of files not written on the position file. */
  public static final String SKIP_TO_END = "skipToEnd";
  public static final boolean DEFAULT_SKIP_TO_END = false;

  /** Time (ms) to close idle files. */
  public static final String IDLE_TIMEOUT = "idleTimeout";
  public static final int DEFAULT_IDLE_TIMEOUT = 120000;

  /** Interval time (ms) to write the last position of each file on the position file. */
  public static final String WRITE_POS_INTERVAL = "writePosInterval";
  public static final int DEFAULT_WRITE_POS_INTERVAL = 3000;
  

  /** Whether to add the byte offset of a tailed line to the header */
  public static final String BYTE_OFFSET_HEADER = "byteOffsetHeader";
  public static final String BYTE_OFFSET_HEADER_KEY = "byteoffset";
  public static final boolean DEFAULT_BYTE_OFFSET_HEADER = false;

  /** Whether to cache the list of files matching the specified file patterns till parent directory
   * is modified.
   */
  public static final String CACHE_PATTERN_MATCHING = "cachePatternMatching";
  public static final boolean DEFAULT_CACHE_PATTERN_MATCHING = true;

  /** Header in which to put absolute path filename. */
  public static final String FILENAME_HEADER_KEY = "fileHeaderKey";
  public static final String DEFAULT_FILENAME_HEADER_KEY = "file";

  /** Whether to include absolute path filename in a header. */
  public static final String FILENAME_HEADER = "fileHeader";
  public static final boolean DEFAULT_FILE_HEADER = false;
  
  /** Filepath where flume watch. */
  public static final String SOURCE_PATH = "sourcePath";
  public static final String DEFAULT_SOURCE_PATH = "/home/tms/inbox";
  /** Filepath where to put sent files. */
  public static final String TARGET_PATH = "targetPath";
  public static final String DEFAULT_TARGET_PATH = "/home/tms/sent";
  
  /** Switch file to write interval. */
  public static final String FILE_SWITCH_INTERVAL = "fileSwitchInterval";
  public static final int DEFAULT_FILE_SWITCH_INTERVAL = 20000;
  
  /** agentStatLog file name without suffix. */
  public static final String AGENT_STAT_LOG_NAME = "agentStatLogName";
  public static final String DEFAULT_AGENT_STAT_LOG_NAME = "agentStatLog";
  
  /** agentStatLog write log heartbeat interval. */
  public static final String WRITE_AGENT_STAT_LOG_INTERVAL = "writeAgentStatLogInterval";
  public static final int DEFAULT_AGENT_WRITE_STAT_LOG_INTERVAL = 60000;
  
  /** archiveSentDirInterval. */
  public static final String ARCHIVE_SENT_DIR_INTERVAL = "archiveSentDirInterval";
  public static final int DEFAULE_ARCHIVE_SENT_DIR_INTERVAL = 3600*1000;
  
  /** deleteSentDirInterval, */
  public static final String DELETE_SENT_DIR_INTERVAL = "deleteSentDirInterval";
  public static final int DEFAULT_DELETE_SENT_DIR_INTERVAL = 7;
  
  /** agent version. */
  public static final String AGENT_VERSION = "agentVersion";
  public static final String DEFAULT_AGENT_VERSION = "version-unknow(based on flume1.7.0)";
  
  /** agentStatLogSwitchInterval. */
  public static final String AGENT_STAT_LOG_SWITCH_INTERVAL = "agentStatLogSwitchInterval";
  public static final int DEFAULT_AGENT_STAT_LOG_SWITCH_INTERVAL = 1;
  
  /**　flume log path. */
  public static final String FLUME_LOG_PATH = "flumeLogPath";
  public static final String DEFAULT_FLUME_LOG_PATH = "/home/tms/flume/logs";
  
}
