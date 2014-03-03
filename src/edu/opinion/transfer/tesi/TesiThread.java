package edu.opinion.transfer.tesi;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * ����ת�Ƶ��߳�
 * 
 * @author ch
 * 
 */
public class TesiThread implements Runnable {

	/**
	 * ���ݴ���Ķ���
	 */
	private TesiTransfer transfer = null;
	/**
	 * �Ƿ�ֹͣ����
	 */
	private boolean isCancel = false;
	/**
	 * �Ƿ���������
	 */
	private boolean isRunning = false;

	/**
	 * ִ���߳�
	 */
	public void run() {
		if(isRunning){
			return;
		}
		isRunning = true;
		TesiConfig config = TesiConfig.getInstance();

		String[] postNames = config.postNames.split(";");
		String[] replyNames = config.replyNames.split(";");
		transfer = new TesiTransfer();

		int count = postNames.length;

		for (int i = 0; i < count; i++) {
			if(isCancel){
				break;
			}
			String tpost = postNames[i];
			String treply = replyNames[i];
			transfer.startTransfer(tpost, treply, TesiConfig.getInstance().GetDays);
		}

		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
		String filename = System.getProperty("user.dir") + "\\logs\\"
				+ format.format(new Date()) + ".log";
		transfer.SaveErrors(filename);
		isRunning = false;
	}

	/**
	 * ֹͣ�߳�
	 */
	public void cancel() {
		isCancel = true;
		transfer.stopTransfer();
	}

	/**
	 * @return the isRunning
	 */
	public boolean isRunning() {
		return isRunning;
	}

}
