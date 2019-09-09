package com.taomee.tms.flume.source;

/**
 * 
 * @author window
 * @brief 定时任务调度接口，所有plugin的定时任务需要由此进行调度
 */

public class TailFileCron {
	
	
	// private static final Logger logger = LoggerFactory.getLogger(TaildirSource.class);
	/**
	 * @brief 依据时间进行调度
	 * @param inbox路径
	 * @param target路径
	 * @param 打包文件时间间隔
	 * @param 删除文件时间间隔
	 * @Param flume日志路径
	 */
	public static void dispatchCron(String sourcePath, 
									String targetPath, 
									int archiveSentDirInterval,
									int deleteSentDirInterval,
									String flumeLogPath) {
		
		// 心跳入口
		// 秒級別时间戳
		Long now = System.currentTimeMillis()/1000;
		
		// TODO 不要分每个文件进行打包，整体打包一个tar.gz包
		// 思考 java线程为什么会挂掉
	    if ((now % (archiveSentDirInterval/1000)) == 40*60) {
			// System.out.println("TailFileCron.dispatchCron()");
			TailFilePlugin.archiveSentFileRegular(sourcePath, targetPath, deleteSentDirInterval);
		}

	}	
	
	/**
	 * @brief 当文件发送完毕，进行文件转移
	 * @param oldFilePath
	 * @param newFilePath
	 */
	public static void moveFileWhenFinished(String oldFilePath, String newFilePath) {
		TailFilePlugin.moveFileWhenFinished(oldFilePath, newFilePath);
	}
	
	/**
	 * @brief 接口，写特殊日志，反应服务器当前的状态
	 * @param agentStatLogPath
	 * @param version
	 */
	
	public static void writeSpecialLog(String agentStatLogPath, String version) {
		TailFilePlugin.writeSpecialLog(agentStatLogPath, version);
	}
	
}
