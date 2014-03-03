package edu.opinion.transfer.tesi;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 数据转移<br>
 * 数据转移的处理流程和逻辑都在这
 * 
 * @author ch
 * 
 */
public class TesiTransfer {
	/* ===================成员变量 ======================= */
	/**
	 * 特思的数据库连接
	 */
	private Connection tesiConn = null;
	private Statement tesiStmt = null;
	/**
	 * 系统的数据库连接
	 */
	private Connection sysConn = null;
	private Statement sysStmt = null;

	/**
	 * 是否停止数据转移
	 */
	private boolean isCanceled = false;	
	List<String> errorList = null;

	/* =================================================== */

	/**
	 * 构造函数
	 */
	public TesiTransfer() {
		initConnection();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#finalize()
	 */
	@Override
	protected void finalize() throws Throwable {
		releaseConnetion();
		super.finalize();
	}

	/**
	 * 初始化数据库连接
	 */
	private void initConnection() {
		if (tesiConn == null) {
			tesiConn = TesiUtils.getTesiConnection();			
		}
		if (sysConn == null) {
			sysConn = TesiUtils.getSysConnection();
		}
		try {
			tesiStmt = tesiConn.createStatement();
			sysStmt = sysConn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		errorList = new ArrayList<String>();
	}

	/**
	 * 释放数据库连接
	 */
	public void releaseConnetion() {
		if(tesiStmt != null){
			try {
				tesiStmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				tesiStmt = null;
			}
		}
		if (tesiConn != null) {
			try {
				tesiConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				tesiConn = null;
			}
		}
		if(sysStmt != null){
			try {
				sysStmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}finally{
				sysStmt = null;
			}
		}
		if (sysConn != null) {
			try {
				sysConn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				sysConn = null;
			}
		}
	}


	
	/**
	 * 开始数据转移
	 * 
	 * @param postName
	 *          视图的名称
	 * @param replyName
	 *          视图的名称
	 * @param getDays
	 *          取得数据的时间
	 */
	public void startTransfer(String postName, String replyName, int getDays) {
		TesiConfig config = TesiConfig.getInstance();
		isCanceled = false;
		int postCount = getTesiCount(postName, 0, getDays);
		int replyCount = getTesiCount(replyName, 1, getDays);
		// 生成数据转移任务
		LinkedBlockingQueue<TransferJob> jobQueue = new LinkedBlockingQueue<TransferJob>();

		TransferJob job = null;
		//post主贴
		int number = postCount / config.EachCount;
		for(int i = 0; i < number; i++){
			job = new TransferJob();
			job.viewName = postName;
			job.startIndex = i * config.EachCount + 1;
			job.endIndex = (i+1) * config.EachCount;
			job.getDays = getDays ;
			job.parentId = 0;
			try {
				jobQueue.put(job);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		if (postCount != 0) {
			job = new TransferJob();
			job.viewName = postName;
			job.startIndex = number * config.EachCount + 1;
			job.endIndex = postCount;
			job.getDays = getDays;
			job.parentId = 0;
			try {
				jobQueue.put(job);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		//rerply 回帖
		number = replyCount / config.EachCount;
		for(int i = 0; i < number; i++){
			job = new TransferJob();
			job.viewName = replyName;
			job.startIndex = i * config.EachCount + 1;
			job.endIndex = (i+1) * config.EachCount;
			job.getDays = getDays;
			job.parentId = 1;
			try {
				jobQueue.put(job);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		if (replyCount != 0) {
			job = new TransferJob();
			job.viewName = replyName;
			job.startIndex = number * config.EachCount + 1;
			job.endIndex = replyCount;
			job.getDays = getDays;
			job.parentId = 1;
			try {
				jobQueue.put(job);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		while (!isCanceled) {
			job = jobQueue.poll();
			if (job == null) {
				break;
			}
			ResultSet rs = getTesiData(job.viewName, job.startIndex, job.endIndex,
					job.parentId, job.getDays);
			if(rs != null)
				SaveToSysTable(rs, job.parentId);
		}
	}
	
	/**
	 * 停止数据转移
	 */
	public void stopTransfer(){
		isCanceled = true;
	}
	
	
	/**
	 * 取得记录条数
	 * @param viewName 视图的名称
	 * @param parentId 父节点id，用来标识主贴和回帖
	 * @param getDays 取多少天之内的数据
	 * @return
	 */
	public int getTesiCount(String viewName, int parentId, int getDays){
		//取得开始和结束时间
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		Date endDate = calendar.getTime();
		calendar.add(Calendar.DAY_OF_YEAR, -getDays);
		Date startDate = calendar.getTime();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");		
		
		int count = 0;
		
		//构造sql语句
		StringBuffer sb = new StringBuffer();
		if(parentId == 0){ //主贴
			sb.append("select count(*) as count from ").append(viewName)
				.append(" where releaseTime between '").append(format.format(startDate))
				.append("' and '").append(format.format(endDate)).append("'");
		}else{ //回帖
			sb.append("select count(*) as count from ").append(viewName)
				.append(" where retime between '").append(format.format(startDate))
				.append("' and '").append(format.format(endDate)).append("'");
		}
		try {
			System.out.println(sb.toString());
			ResultSet rs = tesiStmt.executeQuery(sb.toString());
			if(rs.next()){
				count = rs.getInt("count");
			}
			rs.close();
			rs = null;
		} catch (SQLException e) {
			e.printStackTrace();
			errorList.add(e.getMessage());
		}
		return count;
	}

	/**
	 * 取得特思论坛的数据
	 * 
	 * @param viewName
	 * 					视图的名称
	 * @param startIndex
	 *          开始的记录号
	 * @param endIndex
	 *          结束的记录号
	 * @param parentId
	 *          父节点id
	 * @param getDays
	 *          取几天之内的数据
	 * @return
	 */
	public ResultSet getTesiData(String viewName, int startIndex, int endIndex, 
			int parentId, int getDays){
		//取得开始和结束时间
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		Date endDate = calendar.getTime();
		calendar.add(Calendar.DAY_OF_YEAR, -getDays);
		Date startDate = calendar.getTime();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		//构造sql语句
		StringBuffer sb = new StringBuffer();		
		if(parentId == 0){ //主贴
			sb.append("select * from ")
				.append("(select *, row_number() over(order by id) as rownum")
				.append(" from ").append(viewName)
				.append(" where releaseTime between '").append(format.format(startDate))
				.append("' and '").append(format.format(endDate)).append("'")
				.append(") as tmp")
				.append(" where rownum between ").append(startIndex)
				.append(" and ").append(endIndex);
		}else{ //回帖
			sb.append("select * from ")
				.append("(select *, row_number() over(order by id) as rownum")
				.append(" from ").append(viewName)
				.append(" where reTime between '").append(format.format(startDate))
				.append("' and '").append(format.format(endDate)).append("'")
				.append(") as tmp")
				.append(" where rownum between ").append(startIndex)
				.append(" and ").append(endIndex);
		}
		ResultSet rs = null;
		try {
			System.out.println(sb.toString());
			rs = tesiStmt.executeQuery(sb.toString());
			return rs;
		} catch (SQLException e) {
			rs = null;
			e.printStackTrace();
			errorList.add(e.getMessage());
		}
		return null;
	}
	
	/**
	 * 保存到新的数据库系统中
	 * 
	 * @param rs
	 *          保存特思数据的数据集
	 * @param parentid
	 *          用来判断是主贴还是回帖
	 */
	public void SaveToSysTable(ResultSet rs, int parentid) {
//		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		List<String> sqlList = new ArrayList<String>();
		try {
			while(rs.next()){
				StringBuffer sb = new StringBuffer();
				if(parentid == 0){ //主贴
					sb.append("insert into tb_parser_bbs")
						.append(" (id, topic, author, content, releasetime, renum, tag, url)")
						.append(" values (")
						.append(rs.getInt("id")).append(", '")
						.append(rs.getString("topic")).append("', '")
						.append(rs.getString("author")).append("', '")
						.append(rs.getString("content")).append("', '")
						.append(rs.getDate("releasetime")).append(" ")
						.append(rs.getTime("releasetime")).append("', ")
						.append(rs.getInt("renum")).append(", '")
						.append(rs.getString("tag")).append("', '")
						.append(rs.getString("url")).append("')");
				}else{ //回帖
					sb.append("insert into tb_re_bbs")
						.append(" (id, idofcard, retitle, reauthor, recontent, retime, tag, url)")
						.append(" values (")
						.append(rs.getInt("id")).append(", ")
						.append(rs.getInt("idofcard")).append(", '")
						.append(rs.getString("retitle")).append("', '")
						.append(rs.getString("reauthor")).append("', '")
						.append(rs.getString("recontent")).append("', '")
						.append(rs.getDate("retime")).append(" ").append(rs.getTime("retime")).append("', '")
						.append(rs.getString("tag")).append("', '")
						.append(rs.getString("url")).append("')");
				}
				sqlList.add(sb.toString());
			}
			rs.close();
			rs = null;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		//插入数据
		for(String sql: sqlList){
			try {
				System.out.println(sql);
				sysStmt.execute(sql);
			} catch (SQLException e) {
				errorList.add(e.getMessage() + "\t" + sql);
				System.err.println(e.getMessage());
				continue;
			}			
		}

	}
	
	/**
	 * 保存出现的错误
	 */
	public void SaveErrors(String filename) {
		try {
			FileWriter fWriter = new FileWriter(filename);
			PrintWriter pWriter = new PrintWriter(fWriter);
			for(String error: errorList){
				pWriter.println(error);
			}
			pWriter.close();
			fWriter.close();
			pWriter = null;
			fWriter = null;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 数据转移的任务对象<br>
	 * 主要记录操作的视图、开始行号，结束行号，是否是主贴，取多长时间的数据
	 * 
	 * @author ch
	 * 
	 */
	class TransferJob {
		/**
		 * 操作的视图
		 */
		public String viewName;
		/**
		 * 开始行号
		 */
		public int startIndex;
		/**
		 * 结束行号
		 */
		public int endIndex;
		/**
		 * 是否为主贴，0：主贴，1：回帖
		 */
		public int parentId;
		/**
		 * 取多长时间的数据
		 */
		public int getDays;
	}

}
