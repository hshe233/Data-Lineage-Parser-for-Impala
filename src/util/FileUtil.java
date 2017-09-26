package util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Proxy;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.ChannelSftp.LsEntry;

/**
 * 文件工具类
 * 
 * @author: hshe-161202
 * @create date: 2017年7月14日
 * 
 */
public class FileUtil {

	/**
	 * 构建channelExec，使用代理
	 * 
	 * @param host
	 * @param port
	 * @param username
	 * @param password
	 * @param proxy
	 * @return
	 * @throws JSchException
	 */
	public ChannelExec channelExec(String host, int port, String username, String password, Proxy proxy)
			throws JSchException {

		ChannelExec channelExec = null;
		JSch jsch = new JSch();

		Session session = jsch.getSession(username, host, port);
		session.setProxy(proxy);
		session.setPassword(password);
		Properties config = new Properties();
		config.put("StrictHostKeyChecking", "no");
		session.setConfig(config);
		session.connect();

		Channel channel = session.openChannel("exec");

		channelExec = (ChannelExec) channel;
		return channelExec;
	}

	/**
	 * 构建channelExec，不使用代理
	 * 
	 * @param host
	 * @param port
	 * @param username
	 * @param password
	 * @return
	 * @throws JSchException
	 */
	public ChannelExec channelExec(String host, int port, String username, String password) throws JSchException {

		ChannelExec channelExec = null;
		JSch jsch = new JSch();

		Session session = jsch.getSession(username, host, port);
		session.setPassword(password);
		Properties config = new Properties();
		config.put("StrictHostKeyChecking", "no");
		session.setConfig(config);
		session.connect();

		Channel channel = session.openChannel("exec");

		channelExec = (ChannelExec) channel;
		return channelExec;
	}

	/**
	 * 构建ChannelSftp，使用代理
	 * 
	 * @param host
	 * @param port
	 * @param username
	 * @param password
	 * @param proxy
	 * @return
	 * @throws JSchException
	 */
	public ChannelSftp channelSftp(String host, int port, String username, String password, Proxy proxy)
			throws JSchException {

		ChannelSftp channelSftp = null;
		JSch jsch = new JSch();

		Session session = jsch.getSession(username, host, port);
		session.setProxy(proxy);
		session.setPassword(password);
		Properties config = new Properties();
		config.put("StrictHostKeyChecking", "no");
		session.setConfig(config);
		session.connect();

		Channel channel = session.openChannel("sftp");
		channel.connect();

		channelSftp = (ChannelSftp) channel;
		return channelSftp;
	}

	/**
	 * 构建ChannelSftp，不使用代理
	 * 
	 * @param host
	 * @param port
	 * @param username
	 * @param password
	 * @return
	 * @throws JSchException
	 */
	public ChannelSftp channelSftp(String host, int port, String username, String password) throws JSchException {

		ChannelSftp channelSftp = null;
		JSch jsch = new JSch();

		Session session = jsch.getSession(username, host, port);
		session.setPassword(password);
		Properties config = new Properties();
		config.put("StrictHostKeyChecking", "no");
		session.setConfig(config);
		session.connect();

		Channel channel = session.openChannel("sftp");
		channel.connect();

		channelSftp = (ChannelSftp) channel;
		return channelSftp;
	}

	/**
	 * 列出本地路径下所有文件名
	 * 
	 * @param directory
	 * @return
	 * @throws Exception
	 */
	public List<String> listFiles(String directory) throws Exception {

		String[] fileList = null;
		List<String> fileNameList = new ArrayList<String>();
		try {
			File dir = new File(directory);
			fileList = dir.list();
		} catch (Exception e) {
			return null;
		}
		for (String file : fileList) {
			fileNameList.add(file);
		}

		return fileNameList;
	}

	/**
	 * 列出远程服务器路径下所有文件名
	 * 
	 * @param directory
	 * @param sftp
	 * @return
	 * @throws Exception
	 */
	public List<String> listFiles(String directory, ChannelSftp sftp) throws Exception {

		Vector<?> fileList = null;
		List<String> fileNameList = new ArrayList<String>();
		try {
			fileList = sftp.ls(directory);
		} catch (Exception e) {
			return null;
		}
		Iterator<?> it = fileList.iterator();

		while (it.hasNext()) {
			String fileName = ((LsEntry) it.next()).getFilename();
			if (".".equals(fileName) || "..".equals(fileName)) {
				continue;
			}
			fileNameList.add(fileName);

		}
		return fileNameList;
	}

	/**
	 * 使用channelExec执行command命令
	 * 
	 * @param command
	 * @param channelExec
	 * @throws IOException
	 * @throws JSchException
	 */
	public boolean doShell(String command, ChannelExec channelExec) {

		channelExec.setCommand(command);
		channelExec.setInputStream(null);
		channelExec.setErrStream(System.err);
		InputStream in;

		try {
			channelExec.connect();
		} catch (JSchException e) {
			e.printStackTrace();
			return false;
		}
		try {
			in = channelExec.getInputStream();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(in, Charset.forName("UTF-8")));
		String buf = null;
		StringBuffer sb = new StringBuffer();
		try {
			while ((buf = reader.readLine()) != null) {
				sb.append(buf);
				System.out.println(buf);
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
			channelExec.disconnect();
		}

		return true;
	}

	/**
	 * 使用ChannelSftp下载文件
	 * 
	 * @param directory
	 * @param downloadFile
	 * @param saveDirectory
	 * @param sftp
	 * @return
	 */
	public boolean download(String directory, String downloadFile, String saveDirectory, ChannelSftp sftp) {
		FileOutputStream fos = null;
		try {
			String saveFile = saveDirectory + "/" + downloadFile;
			sftp.cd(directory);
			File savedic = new File(saveDirectory);
			if (!savedic.exists()) {
				savedic.mkdirs();
			}
			File file = new File(saveFile);
			fos = new FileOutputStream(file);
			sftp.get(downloadFile, fos);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return true;
	}

	/**
	 * 下载目录下全部文件
	 * 
	 * @param directory
	 *            下载目录
	 * @param saveDirectory
	 *            存在本地的路径
	 * 
	 * @throws Exception
	 */

	public boolean downloadByDirectory(String directory, String saveDirectory, ChannelSftp sftp) {
		String downloadFile = "";
		boolean flag = false;
		List<String> downloadFileList;
		try {
			downloadFileList = listFiles(directory, sftp);
			Iterator<String> it = downloadFileList.iterator();
			while (it.hasNext()) {
				downloadFile = it.next().toString();
				if (downloadFile.toString().indexOf(".") < 0) {
					continue;
				}
				flag = download(directory, downloadFile, saveDirectory, sftp);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return flag;
		}
		return flag;
	}

	/**
	 * 上传单个文件
	 * 
	 * @param directory
	 *            上传的目录
	 * @param uploadFile
	 *            要上传的文件
	 * @param sftp
	 */
	public boolean upload(String directory, String uploadFile, ChannelSftp sftp) {
		FileInputStream fis = null;
		try {
			try {
				if (sftp.ls(directory) == null) {
					sftp.mkdir(directory);
				}
			} catch (Exception e) {
				sftp.mkdir(directory);
			}
			sftp.cd(directory);
			File file = new File(uploadFile);
			fis = new FileInputStream(file);
			sftp.put(fis, file.getName());
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			if (sftp != null) {
				sftp.disconnect();
			}
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return true;
	}

	private static String message;

	public static String read(String filePathAndName) {
		return read(filePathAndName, "utf-8");
	}

	/**
	 * 读取文本文件内容
	 * 
	 * @param filePathAndName
	 *            带有完整绝对路径的文件名
	 * @param encoding
	 *            文本文件打开的编码方式
	 * @return 返回文本文件的内容
	 */
	public static String read(String filePathAndName, String encoding) {
		StringBuffer str = new StringBuffer("");
		String st = "";
		FileInputStream fs = null;
		InputStreamReader isr = null;
		try {
			fs = new FileInputStream(filePathAndName);
			if (CheckUtil.isEmpty(encoding)) {
				isr = new InputStreamReader(fs);
			} else {
				isr = new InputStreamReader(fs, encoding.trim());
			}
			BufferedReader br = new BufferedReader(isr);
			try {
				String data = "";
				while ((data = br.readLine()) != null) {
					str.append(data + "\n");
				}
			} catch (Exception e) {
				str.append(e.toString());
			}
			st = str.toString();
		} catch (IOException es) {
			st = "";
		} finally {
			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (isr != null) {
				try {
					isr.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return st;
	}

	/**
	 * 读取文本文件内容
	 * 
	 * @param filePathAndName
	 *            带有完整绝对路径的文件名
	 * @param encoding
	 *            文本文件打开的编码方式
	 * @return 返回文本文件的内容
	 */
	public static boolean exist(String file) {
		return new File(file).exists();
	}

	/**
	 * 新建目录
	 * 
	 * @param folderPath
	 *            目录
	 * @return 返回目录创建后的路径
	 */
	public static String createFolder(String folderPath) {
		String txt = folderPath;
		try {
			java.io.File myFilePath = new java.io.File(txt);
			txt = folderPath;
			if (!myFilePath.exists()) {
				myFilePath.mkdir();
			}
		} catch (Exception e) {
			message = "创建目录操作出错";
		}
		return txt;
	}

	/**
	 * 多级目录创建
	 * 
	 * @param folderPath
	 *            准备要在本级目录下创建新目录的目录路径 例如 c:myf
	 * @param paths
	 *            无限级目录参数，各级目录以单数线区分 例如 a|b|c
	 * @return 返回创建文件后的路径 例如 c:myfac
	 */
	public static String createFolders(String folderPath, String paths) {
		String txts = folderPath;
		try {
			String txt;
			txts = folderPath;
			StringTokenizer st = new StringTokenizer(paths, "|");
			while (st.hasMoreTokens()) {
				txt = st.nextToken().trim();
				if (txts.lastIndexOf("/") != -1) {
					txts = createFolder(txts + txt);
				} else {
					txts = createFolder(txts + txt + "/");
				}
			}
		} catch (Exception e) {
			message = "创建目录操作出错！";
		}
		return txts;
	}

	/**
	 * 新建文件
	 * 
	 * @param filePathAndName
	 *            文本文件完整绝对路径及文件名
	 * @param fileContent
	 *            文本文件内容
	 * @return
	 */
	public void createFile(String filePathAndName, String fileContent) {

		try {
			String filePath = filePathAndName;
			filePath = filePath.toString();
			File myFilePath = new File(filePath);
			if (!myFilePath.exists()) {
				myFilePath.createNewFile();
			}
			FileWriter resultFile = new FileWriter(myFilePath);
			PrintWriter myFile = new PrintWriter(resultFile);
			String strContent = fileContent;
			myFile.println(strContent);
			myFile.close();
			resultFile.close();
		} catch (Exception e) {
			message = "创建文件操作出错";
			e.printStackTrace();
		}
	}

	/**
	 * 有编码方式的文件创建
	 * 
	 * @param filePathAndName
	 *            文本文件完整绝对路径及文件名
	 * @param fileContent
	 *            文本文件内容
	 * @param encoding
	 *            编码方式 例如 GBK 或者 UTF-8
	 * @return
	 */
	public static void createFile(String filePathAndName, String fileContent, String encoding) {

		try {
			String filePath = filePathAndName;
			filePath = filePath.toString();
			File myFilePath = new File(filePath);
			if (!myFilePath.exists()) {
				myFilePath.createNewFile();
			}
			PrintWriter myFile = new PrintWriter(myFilePath, encoding);
			String strContent = fileContent;
			myFile.println(strContent);
			myFile.close();
		} catch (Exception e) {
			message = "创建文件操作出错";
		}
	}

	/**
	 * 删除文件
	 * 
	 * @param filePathAndName
	 *            文本文件完整绝对路径及文件名
	 * @return Boolean 成功删除返回true遭遇异常返回false
	 */
	public static boolean delFile(String filePathAndName) {
		boolean bea = false;
		try {
			String filePath = filePathAndName;
			File myDelFile = new File(filePath);
			if (myDelFile.exists()) {
				myDelFile.delete();
				bea = true;
			} else {
				bea = false;
				message = (filePathAndName + "删除文件操作出错");
			}
		} catch (Exception e) {
			message = e.toString();
		}
		return bea;
	}

	/**
	 * 删除文件夹
	 * 
	 * @param folderPath
	 *            文件夹完整绝对路径
	 * @return
	 */
	public static void delFolder(String folderPath) {
		try {
			delAllFile(folderPath); // 删除完里面所有内容
			String filePath = folderPath;
			filePath = filePath.toString();
			java.io.File myFilePath = new java.io.File(filePath);
			myFilePath.delete(); // 删除空文件夹
		} catch (Exception e) {
			message = ("删除文件夹操作出错");
		}
	}

	/**
	 * 删除指定文件夹下所有文件
	 * 
	 * @param path
	 *            文件夹完整绝对路径
	 * @return
	 * @return
	 */
	public static boolean delAllFile(String path) {
		boolean bea = false;
		File file = new File(path);
		if (!file.exists()) {
			return bea;
		}
		if (!file.isDirectory()) {
			return bea;
		}
		String[] tempList = file.list();
		File temp = null;
		for (int i = 0; i < tempList.length; i++) {
			if (path.endsWith(File.separator)) {
				temp = new File(path + tempList[i]);
			} else {
				temp = new File(path + File.separator + tempList[i]);
			}
			if (temp.isFile()) {
				temp.delete();
			}
			if (temp.isDirectory()) {
				delAllFile(path + "/" + tempList[i]);// 先删除文件夹里面的文件
				delFolder(path + "/" + tempList[i]);// 再删除空文件夹
				bea = true;
			}
		}
		return bea;
	}

	/**
	 * 复制整个文件夹的内容
	 * 
	 * @param oldPath
	 *            准备拷贝的目录
	 * @param newPath
	 *            指定绝对路径的新目录
	 * @return
	 */
	public static void copyFolder(String oldPath, String newPath) {
		try {
			new File(newPath).mkdirs(); // 如果文件夹不存在 则建立新文件夹
			File a = new File(oldPath);
			String[] file = a.list();
			File temp = null;
			for (int i = 0; i < file.length; i++) {
				if (oldPath.endsWith(File.separator)) {
					temp = new File(oldPath + file[i]);
				} else {
					temp = new File(oldPath + File.separator + file[i]);
				}
				if (temp.isFile()) {
					FileInputStream input = new FileInputStream(temp);
					FileOutputStream output = new FileOutputStream(newPath + "/" + (temp.getName()).toString());
					byte[] b = new byte[1024 * 5];
					int len;
					while ((len = input.read(b)) != -1) {
						output.write(b, 0, len);
					}
					output.flush();
					output.close();
					input.close();
				}
				if (temp.isDirectory()) {// 如果是子文件夹
					copyFolder(oldPath + "/" + file[i], newPath + "/" + file[i]);
				}
			}
		} catch (Exception e) {
			message = "复制整个文件夹内容操作出错";
		}
	}

	/**
	 * 移动目录
	 * 
	 * @param oldPath
	 * @param newPath
	 * @return
	 */
	public static void moveFolder(String oldPath, String newPath) {
		copyFolder(oldPath, newPath);
		delFolder(oldPath);
	}

	/**
	 * 得到错误信息
	 */
	public static String getMessage() {
		return message;
	}
}
