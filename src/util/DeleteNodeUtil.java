package util;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import bean.NodeLine;
import bean.ResultLine;
import bean.TargetNode;

/**
 * 删除特定节点，并建立新的链接 。入参默认为TargetNode或schema, table, column，可以用%表示通配
 * 
 * @author: hshe-161202
 * @create date: 2017年7月14日
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

		logger.info("开始搜索相关血缘: " + targetNode.toString());
		
		List<ResultLine> list = new ArrayList<ResultLine>();

		try {

			/**
			 * 输入参数作为target做like匹配，获取上层resList1
			 */
			List<ResultLine> resList1 = dbUtil.doSelectLike(targetNode);

			for (ResultLine res1 : resList1) {

				/**
				 * 从下层中提取具体的target，作为source匹配，获取下层resList2
				 */
				List<ResultLine> resList2 = dbUtil.doSelect(res1.getTargetNode().toSourceLine());
				for (ResultLine res2 : resList2) {

					/**
					 * 将匹配到的上层和下层进行组合成新的ResultLine rs
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

			logger.info("搜索完成，共搜索到相关血缘 " + list.size() + " 条。开始建立跨越关系 。。");
			dbUtil.doInsertBatch(list);
			logger.info("跨越关系建立完成，开始删除旧血缘 。。");
			dbUtil.doDelete((NodeLine) targetNode);

			logger.info("执行完毕！");

		} catch (Exception e) {
			logger.error("删除无效血缘发生异常: " + e.getMessage());
			e.printStackTrace();
		} finally {
			dbUtil.close();
		}
	}
}
