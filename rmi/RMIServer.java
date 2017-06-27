package neu.expertInquireSystem.rmi;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import neu.expertInquireSystem.Constants;


public class RMIServer {
  private String address;
  private int port;
  public RMIServer(){
    StringBuffer ipAddressStr=new StringBuffer();
    ipAddressStr.append("rmi://");
    ipAddressStr.append(Constants.defaultIpAddress);
    ipAddressStr.append(":"+Constants.defaultPort+"/RMIServer");
    this.address=ipAddressStr.toString();
    this.port=Constants.defaultPort;
  }
  public RMIServer(String ipAddress,int port){
      StringBuffer ipAddressStr=new StringBuffer();
      ipAddressStr.append("rmi://");
      ipAddressStr.append(ipAddress);
      ipAddressStr.append(":"+port+"/RMIServer");
      this.address=ipAddressStr.toString();
      this.port=port;
  }
  public void start(){
    RMIServerInterface IRMIServer=null;
    try {
      IRMIServer = new RMIServerImp(); 
      LocateRegistry.createRegistry(this.port);
      Naming.rebind(address, IRMIServer);
      System.out.println("RMIServer is ready...");
    } catch (RemoteException e) {
      e.printStackTrace();
    } catch (MalformedURLException e) {
      e.printStackTrace();
    }
  }
}
