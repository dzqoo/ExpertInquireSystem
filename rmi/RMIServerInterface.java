package neu.expertInquireSystem.rmi;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

//import neu.expertInquireSystem.dataManager.ExpertList;
//import neu.expertInquireSystem.dataManager.InquireCondition;
import edu.neu.expert.mvc.pojo.Requirement;

public interface RMIServerInterface extends Remote{
  public List<Map<String, String>> inquireExpert(Requirement inquireCondition) throws RemoteException;
}
