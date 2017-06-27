package neu.expertInquireSystem.rmi;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;


//import neu.expertInquireSystem.dataManager.ExpertList;
import edu.neu.expert.mvc.pojo.Requirement;
import neu.expertInquireSystem.util.SetRunningCondition;
import neu.expertInquireSystem.Constants;

class Inquire extends Thread{
  Requirement inquireCondition;
  public Inquire(Requirement inquireCondition){
    this.inquireCondition=inquireCondition;
  }
  public void run(){
	
	/**
	 * 向RMI服务器发送查询请求，服务器根据查询条件执行查询
	 */
    try {
    	long start = System.currentTimeMillis();
    	
    	StringBuffer ipAddressStr=new StringBuffer();
    	ipAddressStr.append("rmi://");
    	ipAddressStr.append(Constants.defaultIpAddress);
    	ipAddressStr.append(":"+Constants.defaultPort+"/RMIServer");
    	String address=ipAddressStr.toString();
    	RMIServerInterface expert = (RMIServerInterface) Naming.lookup(address);
    	
    	List<Map<String, String>> expertList = expert.inquireExpert(inquireCondition);
    	if(expertList == null){
    		System.out.println("the inquireResult is null...");
    	}else{
    		System.out.println("print the inquireResult ...");
//    		expertList.print();
    		for(Map<String, String> map : expertList){
    			System.out.println(map.toString());
    		}
    	}
    	long stop = System.currentTimeMillis();
    	System.out.println("the query runtime is: " + (stop-start) + " ms");
    } catch (MalformedURLException e) {
    	// TODO Auto-generated catch block
    	e.printStackTrace();
    } catch (RemoteException e) {
    	// TODO Auto-generated catch block
    	e.printStackTrace();
    } catch (NotBoundException e) {
    	// TODO Auto-generated catch block
    	e.printStackTrace();
    }
  }
}

public class Client {

  public static void main(String[] args) {
	  
	/**
	 * 设置查询条件模拟查询请求
	 */
	SetRunningCondition src = new SetRunningCondition();
	
    Requirement inquireCondition1=new Requirement();
    
    src.setExpertcondition1();
    src.setAvoidcondition1();
  
    
    inquireCondition1.setExpertCondition(src.getExpertcondition1());
    inquireCondition1.setAvoidCondition(src.getAvoidcondition1());

    System.out.println(inquireCondition1);
    new Inquire(inquireCondition1).start();
    
  }
}
