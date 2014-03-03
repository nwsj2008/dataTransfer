package edu.opinion.transfer.tesi;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 数据转移的线程
 * 
 * @author ch
 * 
 */
public class TesiThread implements Runnable {

	/**
	 * 数据传输的对象
	 */
	private TesiTransfer transfer = null;
	/**
	 * 是否停止传输
	 */
	private boolean isCancel = false;
	/**
	 * 是否正在运行
	 */
	private boolean isRunning = false;

	/**
	 * 执行线程
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
	 * 停止线程
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
