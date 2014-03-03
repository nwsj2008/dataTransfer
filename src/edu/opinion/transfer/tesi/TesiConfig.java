package edu.opinion.transfer.tesi;

import java.io.File;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;


/**
 * ת����˼���ݵĲ�������
 * 
 * @author ch
 * 
 */
public class TesiConfig {
	/**
	 * ����
	 */
	private static TesiConfig instance = null;
	public boolean isInit = false;
	
	/**
	 * ��ȡ����
	 * @return
	 */
	public static TesiConfig getInstance(){
		if(instance == null){
			instance = new TesiConfig();
		}
		return instance;
	}
	
	/**
	 * ���캯��
	 */
	private TesiConfig(){
		ReadConfig();
	}
	
	/**
	 * ��ȡ����ת�Ƶ������ļ�
	 */
	private void ReadConfig(){
		isInit = false;
		SAXReader reader = new SAXReader();
		String filename = System.getProperty("user.dir") + "\\conf\\tesi.cfg" ;
		try {
			Document document = reader.read(new File(filename));
			Element transferElement = (Element)document.selectNodes("transfer").get(0);
			Element eachcountElement = (Element)transferElement.selectNodes("eachcount").get(0);
			Element getdaysElement = (Element)transferElement.selectNodes("getdays").get(0);
			Element postElement = (Element)transferElement.selectNodes("postnames").get(0);
			Element replyElement = (Element)transferElement.selectNodes("replynames").get(0);
			Element tesiconfigElement = (Element)transferElement.selectNodes("tesiconfig").get(0);
			Element sysconfigElement = (Element)transferElement.selectNodes("sysconfig").get(0);
			Element tesidatasourceElement = (Element)tesiconfigElement.selectNodes("datasource").get(0);
			Element tesidatabaseElement = (Element)tesiconfigElement.selectNodes("database").get(0);
			Element tesiusernameElement = (Element)tesiconfigElement.selectNodes("username").get(0);
			Element tesipasswordElement = (Element)tesiconfigElement.selectNodes("password").get(0);
			Element sysdatasourcElement = (Element)sysconfigElement.selectNodes("datasource").get(0);
			Element sysdatabaseElement = (Element)sysconfigElement.selectNodes("database").get(0);
			Element sysusernameElement = (Element)sysconfigElement.selectNodes("username").get(0);
			Element syspasswordElement = (Element)sysconfigElement.selectNodes("password").get(0);
			
			//�����µ���������
			EachCount = Integer.parseInt(eachcountElement.getTextTrim());
			GetDays = Integer.parseInt(getdaysElement.getTextTrim());
			postNames = postElement.getTextTrim();
			replyNames = replyElement.getTextTrim();
			TesiDataSource = tesidatasourceElement.getTextTrim();
			TesiDatabase = tesidatabaseElement.getTextTrim();
			TesiUsername = tesiusernameElement.getTextTrim();
			TesiPassword = tesipasswordElement.getTextTrim();
			SysDataSource = sysdatasourcElement.getTextTrim();
			SysDatabase = sysdatabaseElement.getTextTrim();
			SysUsername = sysusernameElement.getTextTrim();
			SysPassword = syspasswordElement.getTextTrim();
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		isInit = true;
	}
	
	
	/*======================================================
	 * ��Ա����
	 =======================================================*/
	
	/**
	 * ÿ��ת�Ƶ�������
	 */
	public int EachCount = 10;
	/**
	 * ת�ƶ�����֮�ڵ�����
	 */
	public int GetDays = 10;	
	/**
	 * ������Ӧ����ͼ�������";"�ֿ�
	 */
	public String postNames = "tpost";
	/**
	 * ������Ӧ����ͼ�������";"�ֿ�
	 */
	public String replyNames = "treply";
	
	/*======================================================
	 * ��˹���ݿ��������� 
	 =======================================================*/
	/**
	 * ��˼�����ݿ��������ַ
	 */
	public String TesiDataSource = "192.168.198.119";
	/**
	 * ��˼�����ݿ���
	 */
	public String TesiDatabase = "dvbbs";
	/**
	 * ��˼�����ݿ��û���
	 */
	public String TesiUsername = "dvbbs";
	/**
	 * ��˼�����ݿ��û�����
	 */
	public String TesiPassword = "dvbbs";
	
	/*======================================================
	 * ϵͳ���ݿ���������
	 =======================================================*/
	/**
	 * ϵͳ���ݿ��������ַ
	 */
	public String SysDataSource = "localhost";
	/**
	 * ϵͳ���ݿ���
	 */
	public String SysDatabase = "yqfx";
	/**
	 * ϵͳ���ݿ��û���
	 */
	public String SysUsername = "root";
	/**
	 * ϵͳ���ݿ��û�����
	 */
	public String SysPassword = "root";
}
