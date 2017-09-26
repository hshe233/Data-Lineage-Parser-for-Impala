package util;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import bean.NodeLine;
import bean.ResultLine;
import bean.TargetNode;

/**
 * ɾ���ض��ڵ㣬�������µ����� �����Ĭ��ΪTargetNode��schema, table, column��������%��ʾͨ��
 * 
 * @author: hshe-161202
 * @create date: 2017��7��14��
 * 
 */
public class DeleteNodeUtil {

	private TargetNode targetNode;
	private Logger logger;

	public DeleteNodeUtil(TargetNode targetNode, Logger logger) {
		this.targetNode = targetNode;
		this.logger = logger;
	}

	public DeleteNodeUtil(String schema, String table, String column, Logger logger) {
		this.targetNode = new TargetNode(schema, table, column);
		this.logger = logger;
	}

	public void doDelete(DBUtil dbUtil) {

		logger.info("��ʼ�������ѪԵ: " + targetNode.toString());
		
		List<ResultLine> list = new ArrayList<ResultLine>();

		try {

			/**
			 * ���������Ϊtarget��likeƥ�䣬��ȡ�ϲ�resList1
			 */
			List<ResultLine> resList1 = dbUtil.doSelectLike(targetNode);

			for (ResultLine res1 : resList1) {

				/**
				 * ���²�����ȡ�����target����Ϊsourceƥ�䣬��ȡ�²�resList2
				 */
				List<ResultLine> resList2 = dbUtil.doSelect(res1.getTargetNode().toSourceLine());
				for (ResultLine res2 : resList2) {

					/**
					 * ��ƥ�䵽���ϲ���²������ϳ��µ�ResultLine rs
					 */
					ResultLine rs = new ResultLine(res1.getSourceNode(), res2.getTargetNode());
					if ((res1.getEtlPath() + res1.getEtlName()).equals(res2.getEtlPath() + res2.getEtlName())) {
						rs.setEtlPath(res1.getEtlPath());
						rs.setEtlName(res1.getEtlName());
					} else {
						rs.setEtlPath(res1.getEtlPath() + ";" + res2.getEtlPath());
						rs.setEtlName(res1.getEtlName() + ";" + res2.getEtlName());
					}
					list.add(rs);
				}
			}

			logger.info("������ɣ������������ѪԵ " + list.size() + " ������ʼ������Խ��ϵ ����");
			dbUtil.doInsertBatch(list);
			logger.info("��Խ��ϵ������ɣ���ʼɾ����ѪԵ ����");
			dbUtil.doDelete((NodeLine) targetNode);

			logger.info("ִ����ϣ�");

		} catch (Exception e) {
			logger.error("ɾ����ЧѪԵ�����쳣: " + e.getMessage());
			e.printStackTrace();
		} finally {
			dbUtil.close();
		}
	}
}
