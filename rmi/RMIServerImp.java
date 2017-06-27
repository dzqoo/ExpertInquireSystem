package neu.expertInquireSystem.rmi;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;


//import neu.expertInquireSystem.dataManager.ExpertList;
//import neu.expertInquireSystem.dataManager.InquireCondition;
import edu.neu.expert.mvc.pojo.Requirement;
//import neu.expertInquireSystem.dataManager.InquireResult;
import neu.expertInquireSystem.spark.InquireRDD;


public class RMIServerImp extends UnicastRemoteObject implements RMIServerInterface{
  public RMIServerImp() throws RemoteException{
    super();
  }
  public List<Map<String, String>> inquireExpert(Requirement inquireCondition) throws RemoteException{
    System.out.println("get the inquireCondition...");
    System.out.println(inquireCondition);
    if(inquireCondition == null){
		System.out.println("the inqureCondition is null。。。");
		return null;
    }
    System.out.println("start to inquire...");
    InquireRDD inqueryRDD = new InquireRDD(inquireCondition,Server.getDataSet());
    List<Map<String, String>> result = inqueryRDD.execute();
    System.out.println("end of query...");
    if(result != null){
    	System.out.println("the query is successful...");
    }else{
    	System.out.println("the query is failed...");
    }
    return result;
  }
}
