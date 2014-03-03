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
 * ����ת��<br>
 * ����ת�ƵĴ������̺��߼�������
 * 
 * @author ch
 * 
 */
public class TesiTransfer {
	/* ===================��Ա���� ======================= */
	/**
	 * ��˼�����ݿ�����
	 */
	private Connection tesiConn = null;
	private Statement tesiStmt = null;
	/**
	 * ϵͳ�����ݿ�����
	 */
	private Connection sysConn = null;
	private Statement sysStmt = null;

	/**
	 * �Ƿ�ֹͣ����ת��
	 */
	private boolean isCanceled = false;	
	List<String> errorList = null;

	/* =================================================== */

	/**
	 * ���캯��
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
	 * ��ʼ�����ݿ�����
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
	 * �ͷ����ݿ�����
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
	 * ��ʼ����ת��
	 * 
	 * @param postName
	 *          ��ͼ������
	 * @param replyName
	 *          ��ͼ������
	 * @param getDays
	 *          ȡ�����ݵ�ʱ��
	 */
	public void startTransfer(String postName, String replyName, int getDays) {
		TesiConfig config = TesiConfig.getInstance();
		isCanceled = false;
		int postCount = getTesiCount(postName, 0, getDays);
		int replyCount = getTesiCount(replyName, 1, getDays);
		// ��������ת������
		LinkedBlockingQueue<TransferJob> jobQueue = new LinkedBlockingQueue<TransferJob>();

		TransferJob job = null;
		//post����
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
		
		//rerply ����
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
	 * ֹͣ����ת��
	 */
	public void stopTransfer(){
		isCanceled = true;
	}
	
	
	/**
	 * ȡ�ü�¼����
	 * @param viewName ��ͼ������
	 * @param parentId ���ڵ�id��������ʶ�����ͻ���
	 * @param getDays ȡ������֮�ڵ�����
	 * @return
	 */
	public int getTesiCount(String viewName, int parentId, int getDays){
		//ȡ�ÿ�ʼ�ͽ���ʱ��
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
		
		//����sql���
		StringBuffer sb = new StringBuffer();
		if(parentId == 0){ //����
			sb.append("select count(*) as count from ").append(viewName)
				.append(" where releaseTime between '").append(format.format(startDate))
				.append("' and '").append(format.format(endDate)).append("'");
		}else{ //����
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
	 * ȡ����˼��̳������
	 * 
	 * @param viewName
	 * 					��ͼ������
	 * @param startIndex
	 *          ��ʼ�ļ�¼��
	 * @param endIndex
	 *          �����ļ�¼��
	 * @param parentId
	 *          ���ڵ�id
	 * @param getDays
	 *          ȡ����֮�ڵ�����
	 * @return
	 */
	public ResultSet getTesiData(String viewName, int startIndex, int endIndex, 
			int parentId, int getDays){
		//ȡ�ÿ�ʼ�ͽ���ʱ��
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
		
		//����sql���
		StringBuffer sb = new StringBuffer();		
		if(parentId == 0){ //����
			sb.append("select * from ")
				.append("(select *, row_number() over(order by id) as rownum")
				.append(" from ").append(viewName)
				.append(" where releaseTime between '").append(format.format(startDate))
				.append("' and '").append(format.format(endDate)).append("'")
				.append(") as tmp")
				.append(" where rownum between ").append(startIndex)
				.append(" and ").append(endIndex);
		}else{ //����
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
	 * ���浽�µ����ݿ�ϵͳ��
	 * 
	 * @param rs
	 *          ������˼���ݵ����ݼ�
	 * @param parentid
	 *          �����ж����������ǻ���
	 */
	public void SaveToSysTable(ResultSet rs, int parentid) {
//		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		List<String> sqlList = new ArrayList<String>();
		try {
			while(rs.next()){
				StringBuffer sb = new StringBuffer();
				if(parentid == 0){ //����
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
				}else{ //����
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
		//��������
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
	 * ������ֵĴ���
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
	 * ����ת�Ƶ��������<br>
	 * ��Ҫ��¼��������ͼ����ʼ�кţ������кţ��Ƿ���������ȡ�೤ʱ�������
	 * 
	 * @author ch
	 * 
	 */
	class TransferJob {
		/**
		 * ��������ͼ
		 */
		public String viewName;
		/**
		 * ��ʼ�к�
		 */
		public int startIndex;
		/**
		 * �����к�
		 */
		public int endIndex;
		/**
		 * �Ƿ�Ϊ������0��������1������
		 */
		public int parentId;
		/**
		 * ȡ�೤ʱ�������
		 */
		public int getDays;
	}

}
