package com.taomee.tms.flume.source;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author window
 * @brief 文件的定期更新和打包操作，并定时写心跳日志，让下游服务知道此agent的状态
 */

public class TailFilePlugin {


	private static final Logger logger = LoggerFactory.getLogger(TaildirSource.class);

	// remove days two weeks
	// private static final int remove_days = 14;

	/**
	 * @brief  写一条特殊的日志到指定日志文件
	 * @param　 特殊日志路径，不包括后缀
	 * @param  flume版本
	 */
	public static void writeSpecialLog(String agentStatLogPath, String version) {
		// System.out.println("agentStatLogPath: " + agentStatLogPath);
		Calendar calendar = Calendar.getInstance();
		Long now = calendar.getTimeInMillis();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String agentStatLogPathSuffix = sdf.format(now);
		// System.out.println("agentStatLogPathSuffix :" + agentStatLogPathSuffix);
		try {
			String agentStatLogPathFull = agentStatLogPath
					+ agentStatLogPathSuffix;
			File afs = new File(agentStatLogPathFull);
			if (!afs.exists()) {
				afs.createNewFile();
			}
			writeAgentStatLog(agentStatLogPathFull, version);
		} catch (IOException e) {
			logger.error("Error: " + e.getMessage(), e);
		}
	}

	/**
	 * @brief 移动文件到另一个目录,必须要保证在同一个文件系统下面
	 * @param 文件原路径
	 * @param 文件目标路径
	 */		
	public static void moveFileWhenFinished(String oldFilePath,
			String newFilePath) {

		// TODO 用shell来处理,后续思考洗下有没有更好的解决思路
		// windows 可以用renameTo
		// 确保新路径存在
		File ofs = new File(oldFilePath);
		File nfs = new File(newFilePath);
		Long now = System.currentTimeMillis() / 1000;

		if (!nfs.exists()) {
			if (!nfs.mkdirs()) {
				logger.error("ceate newFilePath failed :" + newFilePath);
				return;
			}
		}
		if (ofs.exists()) {
			if (isWindowsOs()) {
				if (!ofs.renameTo(nfs)) {
					logger.error("Error: " + "rename file failed: " + oldFilePath);
				}
				// linux操作系统
			} else {
				try {
					Path oPath = Paths.get(oldFilePath);
					Path nPath = Paths.get(newFilePath);
					// System.out.println("opath :" + oPath.toString());
					// System.out.println("npath :" + nPath.toString());
					// 加上时间戳进来区分文件，防止move操作失败
					Files.move(oPath,
							nPath.resolve(oPath.getFileName() + "_" + now.toString()));
				} catch (Exception e) {
					System.out.println(e.getMessage());
					e.printStackTrace();
					logger.error("Error: " + e.getMessage(), e);
				}
			}
		}
	}

	/**
	 * @brief 打包和删除sent目录下的日志
	 * @param inbox路径
	 * @param sent路径
	 * @param 删除间隔
	 */
	public static void archiveSentFileRegular(String sourcePath,	
			String targetPath, int deleteSentDirInterval) {

		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		// String dirName = sdf.format(calendar.getTime());
		// file not exist 先创建爱目录
		Long now = calendar.getTimeInMillis();
		String nowHour = sdf.format(now);
		List<File> needTarFileList = new ArrayList<>();
		List<File> needMoveFileList = new ArrayList<>();
		try {
			File tfs = new File(targetPath);
			// assert sent file exists
			if (!tfs.exists()) {
				if (!tfs.mkdirs()) {
					System.out.println("mkdirs error");
					logger.error("create sent path failed :" + targetPath);
				}
			}
			if (tfs.exists()) {
				File[] fileList = tfs.listFiles();

				// int fileNum = tfs.list().length;
				// List<File> tarFileList = new ArrayList<File>();
				for (int i = 0; i < fileList.length; ++i) {
					/// System.out.println("file" + fileList[i].getName());
					if (fileList[i].isFile()) {
							needMoveFileList.add(fileList[i]);
						
					} else if (fileList[i].isDirectory()) {
						if (fileList[i].lastModified() + deleteSentDirInterval * 24 * 3600 * 1000 < now) {
							// System.out.println("deleteInerval" + deleteSentDirInterval*24*3600*1000);
							deletePath(fileList[i].getAbsolutePath());
						}
					}
				}
				
				/// 转移文件前一个小时的文件
				for (File needMoveFile : needMoveFileList) {
					calendar.setTimeInMillis(needMoveFile.lastModified());
					String fileMtime = sdf.format(calendar.getTime());
					String archivePath = targetPath + File.separator + 
							fileMtime.substring(0, 8) + File.separator;
					if (Integer.parseInt(fileMtime) < Integer.parseInt(nowHour)) {
						if (createDirIfNotExists(archivePath)) {
							TailFileCron.moveFileWhenFinished(needMoveFile.getAbsolutePath(), archivePath);
						}
					}
				}
				/// 压缩文件夹中的文件
				/// 第二遍遍历，有些目录可能是在第一遍遍历时加上的，有些可能会删除压缩可能会漏掉
				fileList = tfs.listFiles();
				for (int j = 0; j < tfs.list().length; ++j) {
					
					if (fileList[j].isDirectory()) {
				
						File dicFile = fileList[j];
						needTarFileList.clear();
						File[] tarFileList = dicFile.listFiles();
						// List<File> needTarFileList = new ArrayList<>();
						int tarFileNum = dicFile.list().length;
						// needTarFileList.clear();
						for (int k = 0; k < tarFileNum; ++k) {
							/// .tar 文件需要排除
							if (!tarFileList[k].getName().contains(".tar")) {
								needTarFileList.add(tarFileList[k]);
							}
						}
						if (!needTarFileList.isEmpty()) {
							archiveMultiFiles(needTarFileList, dicFile.getAbsolutePath() 
															+ File.separator + nowHour);
						}
					}
				}
			}
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
			logger.error("Error: " + e.getMessage(), e);
		}
	}

//	/**
//	 * @brief 打包删除flume的日志,刪除最後修改時間再兩周前的日誌,這裡需要特別關注flume日誌的輪轉方式
//	 *        flume 内部存在定期删除的日志的配置，此项配置暂时不需要配置
//	 */
//	public static void deleteFlumeLogRegular(String path, int deleteSentDirInterval) {
//		// Cron函数定时调用，每天晚上执行
//		System.out.println("deleteFlumnLogRegular");
//		Long now = System.currentTimeMillis();
//		// 此路徑通過配置文件配置flume的路勁
//		// Log日誌文件
//		File lfs = new File(path);
//		if (lfs.exists()) {
//			File[] fileList = lfs.listFiles();
//			for (File file : fileList) {
//				if (file.exists()) {
//					if (file.lastModified() + deleteSentDirInterval*24*3600*1000 < now) {
//						System.out.println("deleteInerval" + deleteSentDirInterval*24*3600*1000);
//						removeFile(file.getAbsolutePath());
//					}
//				}
//			}
//		}
//	}

//	/**
//	 * @brief 删除日志文件,递归删除
//	 * @param filepath
//	 */
//	private static void removeFile(String filepath) {
//		// System.out.println("removeFile" + filepath);
//		try {
//			File file = new File(filepath);
//			if (file.exists()) {
//				if (file.isFile()) {
//					file.delete();
//					logger.debug("delete file" + filepath);
//				}
//			} else {
//				// System.out.println("file not exist");
//				logger.error("Error: " + filepath + "not exists");
//			}
//		} catch (Exception e) {
//			logger.error("Error: " + e.getMessage(), e);
//		}
//	}

	/**
	 * @brief 打包数据文件
	 * @param sourcepath
	 * @param targetpath
	 * @throws IOException
	 */
	// TODO 代码优化
	@SuppressWarnings("unused")
	private static void archiveFile(String sourcePath, String targzPath) {
		File sfs = new File(sourcePath);
		if (sfs.exists()) {
			byte[] buf = new byte[1024];
			FileOutputStream fout = null;
			TarArchiveOutputStream tout = null;
			FileInputStream fin = null;

			FileOutputStream gzFile = null;
			GZIPOutputStream gzout = null;
			FileInputStream tarin = null;
			try {
				// 建立文件输出流
				fout = new FileOutputStream(targzPath + ".tar");
				// 建立tar输出流
				tout = new TarArchiveOutputStream(fout);
				// 打开需要压缩的文件
				// path为此文件的全路径
				fin = new FileInputStream(sourcePath);
				TarArchiveEntry tarEn = new TarArchiveEntry(sfs);
				// 重命名，否则打包的文件会含有全路径
				tarEn.setName(sfs.getName());
				tout.putArchiveEntry(tarEn);
				int num;
				while ((num = fin.read(buf, 0, 1024)) != -1) {
					tout.write(buf, 0, num);
				}
				tout.closeArchiveEntry();
				fin.close();
				tout.close();
				fout.close();
				// 建立压缩文件输出流
				gzFile = new FileOutputStream(targzPath + ".tar.gz");
				// 建立gzip压缩文件输出流
				gzout = new GZIPOutputStream(gzFile);
				// 打开需要压缩的文件作为文件输入流
				tarin = new FileInputStream(targzPath + ".tar");
				int len;
				while ((len = tarin.read(buf, 0, 1024)) != -1) {
					gzout.write(buf, 0, len);
				}
				gzout.close();
				gzFile.close();
				tarin.close();
				// 栓出.tar文件
				File tfs = new File(targzPath + ".tar");
				tfs.delete();
				// f.deleteOnExit();
				sfs.delete();
			} catch (FileNotFoundException e) {
				logger.error("can not find the file:" + e.getMessage(), e);
			} catch (IOException e) {
				logger.error("Error: " + e.getMessage(), e);
			} finally {
				try {
					if (null != fin)
						fin.close();
					if (null != fout)
						fout.close();
					// if (null != tout) tout.closeArchiveEntry();

					if (null != tarin)
						tarin.close();
					if (null != gzout)
						gzout.close();
					if (null != gzFile)
						gzFile.close();
				} catch (IOException e) {
					logger.error("Error: " + e.getMessage(), e);
				}
			}
		}
	}

	/**
	 * @brief 打包多个文件
	 * @param sourcepath
	 * @param targetpath
	 * @throws IOException
	 */
	// TODO 代码优化
	
	private static void archiveMultiFiles(List<File> fileLists , String targzPath) {
		// File sfs = new File(sourcePath);	
		
		// if (sfs.exists()) {
			byte[] buf = new byte[1024];
			FileOutputStream fout = null;
			TarArchiveOutputStream tout = null;
			FileInputStream fin = null;
			FileOutputStream gzFile = null;
			GZIPOutputStream gzout = null;
			FileInputStream tarin = null;
			try {
				// 建立文件输出流
				fout = new FileOutputStream(targzPath + ".tar");
				// 建立tar输出流
				tout = new TarArchiveOutputStream(fout);
				// 打开需要压缩的文件
				// path为此文件的全路径
				for (File file: fileLists){
					fin = new FileInputStream(file);
					TarArchiveEntry tarEn = new TarArchiveEntry(file);
				    /// 重命名，否则打包的文件会含有全路径
					tarEn.setName(file.getName());
					tout.putArchiveEntry(tarEn);
					IOUtils.copy(fin, tout);
					tout.closeArchiveEntry();
//					int num;
//					while ((num = fin.read(buf, 0, 1024)) != -1) {
//						tout.write(buf, 0, num);
//					}
					/// 删除源文件
					file.delete();
				}
				if (tout != null) {
					tout.flush();
					tout.close();
				}
				fin.close();
				tout.close();
				fout.close();
				// 建立压缩文件输出流
				gzFile = new FileOutputStream(targzPath + ".tar.gz");
				// 建立gzip压缩文件输出流
				gzout = new GZIPOutputStream(gzFile);
				// 打开需要压缩的文件作为文件输入流
				tarin = new FileInputStream(targzPath + ".tar");
				int len;
				while ((len = tarin.read(buf, 0, 1024)) != -1) {
					gzout.write(buf, 0, len);
				}
				gzout.close();
				gzFile.close();
				tarin.close();
				// 删除.tar文件
				File tfs = new File(targzPath + ".tar");
				tfs.delete();
			} catch (FileNotFoundException e) {
				logger.error("can not find the file:" + e.getMessage(), e);
			} catch (IOException e) {
				logger.error("Error: " + e.getMessage(), e);
			} finally {
				try {
					if (null != fin)
						fin.close();
					if (null != fout)
						fout.close();
//					if (null != tout) 
//						tout.closeArchiveEntry();
					if (null != tarin)
						tarin.close();
					if (null != gzout)
						gzout.close();
					if (null != gzFile)
						gzFile.close();
				} catch (IOException e) {
					logger.error("Error: " + e.getMessage(), e);
				}
			}
		
	}
	/**
	 * @brief 递归删除一个路径下的所有文件
	 * @param path
	 */
	private static void deletePath(String path) {
		try {
			File dfs = new File(path);
			if (dfs.exists()) {
				File[] fileList = dfs.listFiles();
				for (File file : fileList) {
					if (file.isDirectory()) {
						deletePath(file.getName());
						// 最后删除文件夹
						file.delete();
						// System.out.println("delete path :" + file.getName());

					} else if (file.isFile()) {
						file.delete();
						// System.out.println("delete file :" + file.getName());
					}
				}
				// 删除最外层的目录
				dfs.delete();
				
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
			logger.error("Error: " + e.getMessage(), e);
		}

	}

	// /**
	// * @brief 正则表达式，截取一个文件的时间宠
	// * @param 文件名
	// */
	// private static String getFileTimeStamp(String filename) {
	// Pattern pattern = Pattern.compile("\\d{8}");
	// Matcher matcher = pattern.matcher(filename);
	// String timestampString = "";
	// try {
	// if (matcher.find()) {
	// System.out.println(matcher.group());
	// timestampString = matcher.group();
	// // return timestampString;
	// }
	// } catch (Exception e) {
	// // return timestampString;
	// System.out.println(e);
	// // return timestampString;
	// }
	// return timestampString;
	// }

	/**
	 * @brief 创建目录
	 * @parm 目录名
	 */
	private static boolean createDirIfNotExists(String destDirName) {
		File dir = new File(destDirName);
		if (dir.exists()) {

			return true;
		}
		if (!destDirName.endsWith(File.separator)) {
			destDirName = destDirName + File.separator;
		}
		if (dir.mkdirs()) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * @brief 处理agentStatLog，
	 * @param agentStatLog日志路劲
	 * @param flume版本号
	 */
	private static void writeAgentStatLog(String agentStatLogPathFull,
			String version) {
		// 时间戳
		// 文件数目
		// flume 版本号
		File afs = new File(agentStatLogPathFull);
		// 文件数目，防止文件堆积
		int fileNum = afs.getParentFile().listFiles().length;
		Long now = System.currentTimeMillis();

		SimpleDateFormat sfs = new SimpleDateFormat("[HH:mm:ss]");
		String timeStamp = sfs.format(now);

		if (afs.exists()) {
			FileWriter writer = null;
			try {
				writer = new FileWriter(agentStatLogPathFull, true);
				// test log
				String testLog = timeStamp + "\t" + "_fileNum_="
						+ Integer.toString(fileNum) + "\t" + "_version_="
						+ version + "\t" + "_localIp_=" + getLocalIp() + '\n';

				writer.write(testLog);
				writer.close();
			} catch (IOException e) {
				logger.error("Error: " + e.getMessage(), e);
			} finally {
				if (writer != null) {
					try {
						writer.close();
					} catch (IOException e) {
						logger.error("Error: " + e.getMessage(), e);
					}
				}
			}
		}
	}

	/**
	 * @throws SocketException
	 * @brief  获取本机ip, windows和linux的实现
	 * @brief  windows机器上未做测试
	 * 
	 */
	private static String getLocalIp() throws SocketException {

		String localip = null;// 本地IP，如果没有配置外网IP则返回它
		String netip = null;// 外网IP

		try {
			Enumeration<NetworkInterface> netInterfaces = NetworkInterface
					.getNetworkInterfaces();
			InetAddress ip = null;
			boolean finded = false;// 是否找到外网IP
			while (netInterfaces.hasMoreElements() && !finded) {
				NetworkInterface ni = netInterfaces.nextElement();
				Enumeration<InetAddress> address = ni.getInetAddresses();
				while (address.hasMoreElements()) {
					ip = address.nextElement();
					if (!ip.isSiteLocalAddress() && !ip.isLoopbackAddress()
							&& ip.getHostAddress().indexOf(":") == -1) {// 外网IP
						netip = ip.getHostAddress();
						finded = true;
						break;
					} else if (ip.isSiteLocalAddress()
							&& !ip.isLoopbackAddress()
							&& ip.getHostAddress().indexOf(":") == -1) {// 内网IP
						localip = ip.getHostAddress();
					}
				}
			}
		} catch (SocketException e) {
			e.printStackTrace();
		}
		if (netip != null && !"".equals(netip)) {
			return netip;
		} else {
			return localip;
		}
	}

	/**
	 * @brief 判断是否是windows操作系统
	 * @return true 是 windows false 不是windows
	 */
	private static boolean isWindowsOs() {
		String os = System.getProperty("os.name");
		// System.out.println("os:" + os);
		if (os.toLowerCase().startsWith("win")) {
			return true;
		} else {
			return false;
		}
	}
}
