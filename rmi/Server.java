package neu.expertInquireSystem.rmi;

//import neu.expertInquireSystem.dataManager.InquireResult;
import neu.expertInquireSystem.spark.DataSet;
import neu.expertInquireSystem.Constants;

public class Server {
	
  //private static InquireResult inquireResult=new InquireResult();
  private static DataSet dataSet=new DataSet();
  
  public static DataSet getDataSet(){
    return dataSet;
  }
  /*
  public static InquireResult getInquireResult(){
    return inquireResult;
  }
  */

  public static void main(String[] args) {
	/**
	 * 数据加载阶段
	 */
	dataSet.loadData();

	/**
	 * 启动RMI服务等待执行查询请求
	 */
	System.setProperty("java.rmi.server.hostname",Constants.defaultIpAddress);
    RMIServer server=new RMIServer();
    server.start();
  }
}