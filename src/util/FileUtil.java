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
 * �ļ�������
 * 
 * @author: hshe-161202
 * @create date: 2017��7��14��
 * 
 */
public class FileUtil {

	/**
	 * ����channelExec��ʹ�ô���
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
	 * ����channelExec����ʹ�ô���
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
	 * ����ChannelSftp��ʹ�ô���
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
	 * ����ChannelSftp����ʹ�ô���
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
	 * �г�����·���������ļ���
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
	 * �г�Զ�̷�����·���������ļ���
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
	 * ʹ��channelExecִ��command����
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
	 * ʹ��ChannelSftp�����ļ�
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
	 * ����Ŀ¼��ȫ���ļ�
	 * 
	 * @param directory
	 *            ����Ŀ¼
	 * @param saveDirectory
	 *            ���ڱ��ص�·��
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
	 * �ϴ������ļ�
	 * 
	 * @param directory
	 *            �ϴ���Ŀ¼
	 * @param uploadFile
	 *            Ҫ�ϴ����ļ�
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
	 * ��ȡ�ı��ļ�����
	 * 
	 * @param filePathAndName
	 *            ������������·�����ļ���
	 * @param encoding
	 *            �ı��ļ��򿪵ı��뷽ʽ
	 * @return �����ı��ļ�������
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
	 * ��ȡ�ı��ļ�����
	 * 
	 * @param filePathAndName
	 *            ������������·�����ļ���
	 * @param encoding
	 *            �ı��ļ��򿪵ı��뷽ʽ
	 * @return �����ı��ļ�������
	 */
	public static boolean exist(String file) {
		return new File(file).exists();
	}

	/**
	 * �½�Ŀ¼
	 * 
	 * @param folderPath
	 *            Ŀ¼
	 * @return ����Ŀ¼�������·��
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
			message = "����Ŀ¼��������";
		}
		return txt;
	}

	/**
	 * �༶Ŀ¼����
	 * 
	 * @param folderPath
	 *            ׼��Ҫ�ڱ���Ŀ¼�´�����Ŀ¼��Ŀ¼·�� ���� c:myf
	 * @param paths
	 *            ���޼�Ŀ¼����������Ŀ¼�Ե��������� ���� a|b|c
	 * @return ���ش����ļ����·�� ���� c:myfac
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
			message = "����Ŀ¼��������";
		}
		return txts;
	}

	/**
	 * �½��ļ�
	 * 
	 * @param filePathAndName
	 *            �ı��ļ���������·�����ļ���
	 * @param fileContent
	 *            �ı��ļ�����
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
			message = "�����ļ���������";
			e.printStackTrace();
		}
	}

	/**
	 * �б��뷽ʽ���ļ�����
	 * 
	 * @param filePathAndName
	 *            �ı��ļ���������·�����ļ���
	 * @param fileContent
	 *            �ı��ļ�����
	 * @param encoding
	 *            ���뷽ʽ ���� GBK ���� UTF-8
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
			message = "�����ļ���������";
		}
	}

	/**
	 * ɾ���ļ�
	 * 
	 * @param filePathAndName
	 *            �ı��ļ���������·�����ļ���
	 * @return Boolean �ɹ�ɾ������true�����쳣����false
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
				message = (filePathAndName + "ɾ���ļ���������");
			}
		} catch (Exception e) {
			message = e.toString();
		}
		return bea;
	}

	/**
	 * ɾ���ļ���
	 * 
	 * @param folderPath
	 *            �ļ�����������·��
	 * @return
	 */
	public static void delFolder(String folderPath) {
		try {
			delAllFile(folderPath); // ɾ����������������
			String filePath = folderPath;
			filePath = filePath.toString();
			java.io.File myFilePath = new java.io.File(filePath);
			myFilePath.delete(); // ɾ�����ļ���
		} catch (Exception e) {
			message = ("ɾ���ļ��в�������");
		}
	}

	/**
	 * ɾ��ָ���ļ����������ļ�
	 * 
	 * @param path
	 *            �ļ�����������·��
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
				delAllFile(path + "/" + tempList[i]);// ��ɾ���ļ���������ļ�
				delFolder(path + "/" + tempList[i]);// ��ɾ�����ļ���
				bea = true;
			}
		}
		return bea;
	}

	/**
	 * ���������ļ��е�����
	 * 
	 * @param oldPath
	 *            ׼��������Ŀ¼
	 * @param newPath
	 *            ָ������·������Ŀ¼
	 * @return
	 */
	public static void copyFolder(String oldPath, String newPath) {
		try {
			new File(newPath).mkdirs(); // ����ļ��в����� �������ļ���
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
				if (temp.isDirectory()) {// ��������ļ���
					copyFolder(oldPath + "/" + file[i], newPath + "/" + file[i]);
				}
			}
		} catch (Exception e) {
			message = "���������ļ������ݲ�������";
		}
	}

	/**
	 * �ƶ�Ŀ¼
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
	 * �õ�������Ϣ
	 */
	public static String getMessage() {
		return message;
	}
}
