package edu.opinion.transfer.tesi;

import java.io.File;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;


/**
 * 转移特思数据的参数配置
 * 
 * @author ch
 * 
 */
public class TesiConfig {
	/**
	 * 单例
	 */
	private static TesiConfig instance = null;
	public boolean isInit = false;
	
	/**
	 * 获取单例
	 * @return
	 */
	public static TesiConfig getInstance(){
		if(instance == null){
			instance = new TesiConfig();
		}
		return instance;
	}
	
	/**
	 * 构造函数
	 */
	private TesiConfig(){
		ReadConfig();
	}
	
	/**
	 * 读取数据转移的配置文件
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
			
			//设置新的配置数据
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
	 * 成员变量
	 =======================================================*/
	
	/**
	 * 每次转移的数据量
	 */
	public int EachCount = 10;
	/**
	 * 转移多少天之内的帖子
	 */
	public int GetDays = 10;	
	/**
	 * 主贴对应的视图，多个用";"分开
	 */
	public String postNames = "tpost";
	/**
	 * 回帖对应的视图，多个用";"分开
	 */
	public String replyNames = "treply";
	
	/*======================================================
	 * 特斯数据库连接配置 
	 =======================================================*/
	/**
	 * 特思的数据库服务器地址
	 */
	public String TesiDataSource = "192.168.198.119";
	/**
	 * 特思的数据库名
	 */
	public String TesiDatabase = "dvbbs";
	/**
	 * 特思的数据库用户名
	 */
	public String TesiUsername = "dvbbs";
	/**
	 * 特思的数据库用户密码
	 */
	public String TesiPassword = "dvbbs";
	
	/*======================================================
	 * 系统数据库连接配置
	 =======================================================*/
	/**
	 * 系统数据库服务器地址
	 */
	public String SysDataSource = "localhost";
	/**
	 * 系统数据库名
	 */
	public String SysDatabase = "yqfx";
	/**
	 * 系统数据库用户名
	 */
	public String SysUsername = "root";
	/**
	 * 系统数据库用户密码
	 */
	public String SysPassword = "root";
}
