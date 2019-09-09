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

import static com.taomee.tms.flume.source.TaildirSourceConfigurationConstants.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.math.Fraction;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.PollableSourceConstants;
import org.apache.flume.source.SyslogUDPSource.syslogHandler;
import org.apache.thrift.transport.TFileTransport.tailPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;

public class TaildirSource extends AbstractSource implements
    PollableSource, Configurable {

  private static final Logger logger = LoggerFactory.getLogger(TaildirSource.class);
  
  
  // TailFileCron类
  // private static TailFileCron croner = new TailFileCron();
  // TODO 单例模式
  // TailFilePlugin类
  //private static TailFilePlugin plugin = new TailFilePlugin();
  
  private Map<String, String> filePaths;
  private Table<String, String, String> headerTable;
  private int batchSize;
  private String positionFilePath;
  private boolean skipToEnd;
  private boolean byteOffsetHeader;

  private SourceCounter sourceCounter;
  private ReliableTaildirEventReader reader;
  private ScheduledExecutorService idleFileChecker;
  private ScheduledExecutorService positionWriter;
  private ScheduledExecutorService pluginExecutor;
  private ScheduledExecutorService specialLogWriter;
  private int retryInterval = 1000;
  private int maxRetryInterval = 5000;
  private int idleTimeout;
  private int checkIdleInterval = 5000;
  private int writePosInitDelay = 5000;
  // 測試的時候，此參數並未加上
  // private int archiveSentDirInterval = 24 * 60 * 60 * 60; 
  private int writePosInterval;
  private boolean cachePatternMatching;

  private List<Long> existingInodes = new CopyOnWriteArrayList<Long>();
  private List<Long> idleInodes = new CopyOnWriteArrayList<Long>();
  private Long backoffSleepIncrement;
  private Long maxBackOffSleepInterval;
  private boolean fileHeader;
  private String fileHeaderKey;
  
  // 切換時間,默认为20s
  private Integer   fileSwitchInterval;
  // 此路徑拿到監控路徑
  private String sourcePath;
  // 此路徑一般為sent路徑
  private String targetPath;
  // 心跳日誌文件文件名
  private String agentStatLogName;
  // 開發版本號 
  // private String version;
  // 打包时间间隔,默认为一个小时
  private Integer archiveSentDirInterval;
  // 文件保留的天数,默认为一周
  private Integer deleteSentDirInterval;
  // 写心跳日志的时间间隔 ,默认为一分钟 
  // private Integer writeAgentStatLogInterval;
  // 特殊日志文件切换时间间隔，默认为一天
  private Integer agentStatLogSwitchInterval;
  // flume自身日志路径
  private String  flumeLogPath;
  

  @Override
  public synchronized void start() {
    logger.info("{} TaildirSource source starting with directory: {}", getName(), filePaths);
    try {
      reader = new ReliableTaildirEventReader.Builder()
          .filePaths(filePaths)
          .headerTable(headerTable)
          .positionFilePath(positionFilePath)
          .skipToEnd(skipToEnd)
          .addByteOffset(byteOffsetHeader)
          .cachePatternMatching(cachePatternMatching)
          .annotateFileName(fileHeader)
          .fileNameHeader(fileHeaderKey)
          .build();
    } catch (IOException e) {
      throw new FlumeException("Error instantiating ReliableTaildirEventReader", e);
    }
    idleFileChecker = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("idleFileChecker").build());
    idleFileChecker.scheduleWithFixedDelay(new idleFileCheckerRunnable(),
        idleTimeout, checkIdleInterval, TimeUnit.MILLISECONDS);

    positionWriter = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("positionWriter").build());
    positionWriter.scheduleWithFixedDelay(new PositionWriterRunnable(),
        writePosInitDelay, writePosInterval, TimeUnit.MILLISECONDS);
    
    // 新增，此线程执行定期打包目录的任务
    pluginExecutor = Executors.newSingleThreadScheduledExecutor(
    	new ThreadFactoryBuilder().setNameFormat("pluginExecutor").build());
    pluginExecutor.scheduleWithFixedDelay(new TailFilePluginRunnable(), 
    	0, 1, TimeUnit.SECONDS);
    
    // 新增，此线程执行发送定期心跳的任务
//    specialLogWriter = Executors.newSingleThreadScheduledExecutor(
//    	new ThreadFactoryBuilder().setNameFormat("SpecialLogWriter").build());
//    pluginExecutor.scheduleWithFixedDelay(new SpecialLogWriterRunable(), 
//    	0, writeAgentStatLogInterval, TimeUnit.MILLISECONDS);
    
    super.start();
    logger.debug("TaildirSource started");
    sourceCounter.start();
    
    // TODO 一些初始化的操作,心跳和先关其他数据
  }

  @Override
  public synchronized void stop() {
    try {
      super.stop();
      ExecutorService[] services = {idleFileChecker, positionWriter, pluginExecutor};
      for (ExecutorService service : services) {
        service.shutdown();
        if (!service.awaitTermination(1, TimeUnit.SECONDS)) {
          service.shutdownNow();
        }
      }
      // write the last position
      writePosition();
      reader.close();
    } catch (InterruptedException e) {
      logger.info("Interrupted while awaiting termination", e);
    } catch (IOException e) {
      logger.info("Failed: " + e.getMessage(), e);
    }
    sourceCounter.stop();
    logger.info("Taildir source {} stopped. Metrics: {}", getName(), sourceCounter);
    // TODO cancel相关线程
    // croner.stop();
  }

  @Override
  public String toString() {
    return String.format("Taildir source: { positionFile: %s, skipToEnd: %s, "
        + "byteOffsetHeader: %s, idleTimeout: %s, writePosInterval: %s }",
        positionFilePath, skipToEnd, byteOffsetHeader, idleTimeout, writePosInterval);
  }

  @Override
  // 可以保证线程安全
  public synchronized void configure(Context context) {
    String fileGroups = context.getString(FILE_GROUPS);
    Preconditions.checkState(fileGroups != null, "Missing param: " + FILE_GROUPS);

    filePaths = selectByKeys(context.getSubProperties(FILE_GROUPS_PREFIX),
                             fileGroups.split("\\s+"));
    Preconditions.checkState(!filePaths.isEmpty(),
        "Mapping for tailing files is empty or invalid: '" + FILE_GROUPS_PREFIX + "'");

    String homePath = System.getProperty("user.home").replace('\\', '/');
    positionFilePath = context.getString(POSITION_FILE, homePath + DEFAULT_POSITION_FILE);
    Path positionFile = Paths.get(positionFilePath);
    try {
      Files.createDirectories(positionFile.getParent());
    } catch (IOException e) {
      throw new FlumeException("Error creating positionFile parent directories", e);
    }
    headerTable = getTable(context, HEADERS_PREFIX);
    batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    skipToEnd = context.getBoolean(SKIP_TO_END, DEFAULT_SKIP_TO_END);
    byteOffsetHeader = context.getBoolean(BYTE_OFFSET_HEADER, DEFAULT_BYTE_OFFSET_HEADER);
    idleTimeout = context.getInteger(IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT);
    writePosInterval = context.getInteger(WRITE_POS_INTERVAL, DEFAULT_WRITE_POS_INTERVAL);
    cachePatternMatching = context.getBoolean(CACHE_PATTERN_MATCHING,
        DEFAULT_CACHE_PATTERN_MATCHING);

    backoffSleepIncrement = context.getLong(PollableSourceConstants.BACKOFF_SLEEP_INCREMENT,
        PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);
    maxBackOffSleepInterval = context.getLong(PollableSourceConstants.MAX_BACKOFF_SLEEP,
        PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP);
    fileHeader = context.getBoolean(FILENAME_HEADER,
            DEFAULT_FILE_HEADER);
    fileHeaderKey = context.getString(FILENAME_HEADER_KEY,
            DEFAULT_FILENAME_HEADER_KEY);
    
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
    
    // 增加的配置项
    sourcePath = context.getString(SOURCE_PATH, DEFAULT_SOURCE_PATH);
    targetPath = context.getString(TARGET_PATH, DEFAULT_TARGET_PATH);
    fileSwitchInterval = context.getInteger(FILE_SWITCH_INTERVAL, DEFAULT_FILE_SWITCH_INTERVAL);
    agentStatLogName = context.getString(AGENT_STAT_LOG_NAME, DEFAULT_AGENT_STAT_LOG_NAME);
    // archiveSentDirInterval = context.getLong(ARCHIVE_SENT_DIR_INTERVAL, DEFAULT_AGENT_STAT_LOG_NAME);
    archiveSentDirInterval = context.getInteger(ARCHIVE_SENT_DIR_INTERVAL, DEFAULE_ARCHIVE_SENT_DIR_INTERVAL);
    deleteSentDirInterval = context.getInteger(DELETE_SENT_DIR_INTERVAL, DEFAULT_DELETE_SENT_DIR_INTERVAL);
    // writeAgentStatLogInterval = context.getInteger(WRITE_AGENT_STAT_LOG_INTERVAL, DEFAULT_DELETE_SENT_DIR_INTERVAL);
    // version = context.getString(AGENT_VERSION, DEFAULT_AGENT_VERSION);
    agentStatLogSwitchInterval = context.getInteger(AGENT_STAT_LOG_SWITCH_INTERVAL, DEFAULT_AGENT_STAT_LOG_SWITCH_INTERVAL);
    flumeLogPath = context.getString(FLUME_LOG_PATH, DEFAULT_FLUME_LOG_PATH);
  }

  private Map<String, String> selectByKeys(Map<String, String> map, String[] keys) {
    Map<String, String> result = Maps.newHashMap();
    for (String key : keys) {
      if (map.containsKey(key)) {
        result.put(key, map.get(key));
      }
    }
    return result;
  }

  private Table<String, String, String> getTable(Context context, String prefix) {
    Table<String, String, String> table = HashBasedTable.create();
    for (Entry<String, String> e : context.getSubProperties(prefix).entrySet()) {
      String[] parts = e.getKey().split("\\.", 2);
      table.put(parts[0], parts[1], e.getValue());
    }
    return table;
  }

  @VisibleForTesting
  protected SourceCounter getSourceCounter() {
    return sourceCounter;
  }

  @Override
  public Status process() {
    Status status = Status.READY;
    
    try {
      existingInodes.clear();
      existingInodes.addAll(reader.updateTailFiles());
      for (long inode : existingInodes) {
        TailFile tf = reader.getTailFiles().get(inode);
        if (tf.needTail()) {
          tailFileProcess(tf, true);
        } 
        // 转移文件, tf 可能没有初始化，tf.getRaf可能返回null
        // 此函数仅仅适用于日志文件会定期切换的场景
        processSentFiles(tf);
      }
      closeTailFiles();
      try {
        TimeUnit.MILLISECONDS.sleep(retryInterval);
      } catch (InterruptedException e) {
        logger.info("Interrupted while sleeping");
      }
    } catch (Throwable t) {
      logger.error("Unable to tail files", t);
      status = Status.BACKOFF;
    }
    return status;
  }

  @Override
  public long getBackOffSleepIncrement() {
    return backoffSleepIncrement;
  }

  @Override
  public long getMaxBackOffSleepInterval() {
    return maxBackOffSleepInterval;
  }

  private void tailFileProcess(TailFile tf, boolean backoffWithoutNL)
      throws IOException, InterruptedException {
    while (true) {
      reader.setCurrentFile(tf);
      List<Event> events = reader.readEvents(batchSize, backoffWithoutNL);
      if (events.isEmpty()) {
        break;
      }
      sourceCounter.addToEventReceivedCount(events.size());
      sourceCounter.incrementAppendBatchReceivedCount();
      try {
        getChannelProcessor().processEventBatch(events);
        reader.commit();
      } catch (ChannelException ex) {
        logger.warn("The channel is full or unexpected failure. " +
            "The source will try again after " + retryInterval + " ms");
        TimeUnit.MILLISECONDS.sleep(retryInterval);
        retryInterval = retryInterval << 1;
        retryInterval = Math.min(retryInterval, maxRetryInterval);
        continue;
      }
      retryInterval = 1000;
      sourceCounter.addToEventAcceptedCount(events.size());
      sourceCounter.incrementAppendBatchAcceptedCount();
      if (events.size() < batchSize) {
        break;
      }
    }
  }

  private void closeTailFiles() throws IOException, InterruptedException {
    for (long inode : idleInodes) {
      TailFile tf = reader.getTailFiles().get(inode);
      if (tf.getRaf() != null) { // when file has not closed yet
        tailFileProcess(tf, false);
        tf.close();
        logger.info("Closed file: " + tf.getPath() + ", inode: " + inode + ", pos: " + tf.getPos());
      }
    }
    idleInodes.clear();
  }

  /**
   * Runnable class that checks whether there are files which should be closed.
   */
  private class idleFileCheckerRunnable implements Runnable {
    @Override
    public void run() {
      try {
        long now = System.currentTimeMillis();
        for (TailFile tf : reader.getTailFiles().values()) {
          if (tf.getLastUpdated() + idleTimeout < now && tf.getRaf() != null) {
            idleInodes.add(tf.getInode());
          }
        }
      } catch (Throwable t) {
        logger.error("Uncaught exception in IdleFileChecker thread", t);
      }
    }
  }

  /**
   * Runnable class that writes a position file which has the last read position
   * of each file.
   */
  private class PositionWriterRunnable implements Runnable {
    @Override
    public void run() {
      writePosition();
    }
  }
  
  /**
   * Runnable class that package files witch has been sent successfully and remove files whose last 
   * modified time is two weeks ago
   */
  private class TailFilePluginRunnable implements Runnable {
	@Override
	public void run() {
	    // System.out.println("click one time");
		TailFileCron.dispatchCron(sourcePath, 
				  					targetPath, 
				  				    archiveSentDirInterval,
				  				    deleteSentDirInterval,
				  				    flumeLogPath);
	  }
  }
  
  /**
   * Runable class that write a special log regularly
   */
  
//  private class SpecialLogWriterRunable implements Runnable {
//	 @Override
//	 public void run() {
//		 TailFilePlugin.writeSpecialLog(sourcePath + File.separator + agentStatLogName, version);
//	 }
//  }
  
  private void writePosition() {
    File file = new File(positionFilePath);
    FileWriter writer = null;
    try {
      writer = new FileWriter(file);
      if (!existingInodes.isEmpty()) {
        String json = toPosInfoJson();
        writer.write(json);
      }
    } catch (Throwable t) {
      logger.error("Failed writing positionFile", t);
    } finally {
      try {
        if (writer != null) writer.close();
      } catch (IOException e) {
        logger.error("Error: " + e.getMessage(), e);
      }
    }
  }

  private String toPosInfoJson() {
    @SuppressWarnings("rawtypes")
    List<Map> posInfos = Lists.newArrayList();
    for (Long inode : existingInodes) {
      TailFile tf = reader.getTailFiles().get(inode);
      posInfos.add(ImmutableMap.of("inode", inode, "pos", tf.getPos(), "file", tf.getPath()));
    }
    return new Gson().toJson(posInfos);
  }
  
  /**
   * @brief 处理已经发送完毕的数据日志文件
   * @param TailFile 文件结构
   */
  private void processSentFiles(TailFile tf) {
	   Long now = System.currentTimeMillis();
	   try {
		   // 每个tf维护一个randomaccessfile, 但是,并不是每个tf都已经和一个文件绑定
		   // 判断文件被处理完,需要判断文件本身的属性，仅仅凭借tf所维护的状态是不可靠 
		   if (tf != null) {
			   File fs = new File(tf.getPath());
			   // System.out.println("fileName: " + fs.getName());
			   // if (tf.getPos() == tf.getRaf().length()) {
			   // tf所对应的文件可能已经close，
		       if (tf.getPos() == fs.length()) {
		    	   // System.out.println(agentStatLogName.subSequence(0, agentStatLogName.length()));
			       	if ((fs.getName().length() >= agentStatLogName.length())
			       			&& fs.getName().subSequence(0, agentStatLogName.length()).equals(agentStatLogName)) {
			       		// System.out.println("agentStatLogName :" + agentStatLogName.substring(0, agentStatLogName.length()));
			       		// 转移一天之前的特殊日志,测试
			       	    if (now - fs.lastModified() > (agentStatLogSwitchInterval*24*3600*1000 + 1000)) {
			       		// if ((now - fs.lastModified()) > 60000) {
			       			TailFileCron.moveFileWhenFinished(tf.getPath(), targetPath + File.separator);
			       		}
			       	// 转移20s之前的游戏日志
			       	} else if (now - fs.lastModified() > (fileSwitchInterval + 1000)) {
			       		TailFileCron.moveFileWhenFinished(tf.getPath(), targetPath + File.separator);
			       	}
		       }
		   }
	   } catch (Exception e) {
		   logger.error("Error: " + e.getMessage(), e);
	   }
  }
  
  
}
