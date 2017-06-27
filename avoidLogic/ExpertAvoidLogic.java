package neu.expertInquireSystem.avoidLogic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

public class ExpertAvoidLogic implements Serializable {
	/**
     * 
     * 判断专家领域是否符合
     * @return boolean
     */
    public static boolean isDomainDir(String[][] dom,boolean flag,String zjxk,String zjxk_jy,String zjxk_jj,String zjxkly){
    	String delim = ";";
    	ArrayList<String> al_domain = new ArrayList<String>();
    	if(!isDomNull(zjxk)){
    		StringTokenizer st = new StringTokenizer(zjxk, delim);
    		while(st.hasMoreTokens()){
    			al_domain.add(st.nextToken());
    		}
    	}
    	
    	if(!isDomNull(zjxk_jy)){
    		StringTokenizer st = new StringTokenizer(zjxk_jy, delim);
    		while(st.hasMoreTokens()){
    			al_domain.add(st.nextToken());
    		}
    	}
    	
    	if(!isDomNull(zjxk_jj)){
    		StringTokenizer st = new StringTokenizer(zjxk_jj, delim);
    		while(st.hasMoreTokens()){
    			al_domain.add(st.nextToken());
    		}
    	}
    	
    	if(!isDomNull(zjxkly)){
    		StringTokenizer st = new StringTokenizer(zjxkly, delim);
    		while(st.hasMoreTokens()){
    			al_domain.add(st.nextToken());
    		}
    	}
    	
    	if(flag){
    		for(int i=0;i<dom.length;i++){
    			boolean f=false;
    			Iterator<String> it = al_domain.iterator();
    			while(it.hasNext()){
    				String s = it.next();
    				if(s.startsWith(dom[i][dom[i].length-1].trim())){
    					f=true;
    					break;
    				}
    			}
    			if(!f){
    				return false;
    			}
    		}
    		return true;
    	}
    	else{
    		for(int i=0;i<dom.length;i++){
    			Iterator<String> it = al_domain.iterator();
    			while(it.hasNext()){
    				String s = it.next();
    				String d = dom[i][dom[i].length-1].trim().split("-")[0];
    				if(s.startsWith(d)){
    					return true;
    				}
    			}
    		}
    	}
    	return false;
    }
    
    public static boolean isDomNull(String s){
    	if(s==null||s.trim().equals("")||s.equalsIgnoreCase("null")){
    		return true;
    	}
    	return false;
    }
    
    /**
     * 
     * 判断工作性质是否符合
     * @return boolean
     */
    public static boolean isWorkProperty(String condition,String workType){
    	String delim = ";";
    	if(isDomNull(condition)){
    		return true;
    	}
    	else{
    		if(isDomNull(workType)){
    			return false;
    		}
    		String[] workType_a=workType.split(delim);
    		for(int i=0;i<workType_a.length;i++){
    			if(condition.indexOf(workType_a[i].trim())>=0){
        			return true;
        		}
    		}
    	}
    	return false;
    }
    
    /**
     * 
     * 判断职称是否符合
     * @return boolean
     */
    public static boolean isTitle(String condition,String title){
    	if(isDomNull(condition)||condition.equals("25")){
    		return true;
    	}
    	else{
    		if(isDomNull(title)){
    			return false;
    		}
    		if(condition.indexOf(title.trim())>=0){
    			return true;
    		}
    	}
    	return false;
    }
    
    /**
     * 
     * 判断执业资格是否符合
     * @return boolean
     */
    public static boolean isMIPA(String condition,String certify){
    	if(isDomNull(condition)||condition.equals("99")){
    		return true;
    	}
    	else{
    		if(isDomNull(certify)){
    			return false;
    		}
    		if(condition.indexOf(certify.trim())>=0){
    			return true;
    		}
    	}
    	return false;
    }
    
    /**
     * 
     * 判断单位类型是否符合
     * @return boolean
     */
    public static boolean isCompanyType(String condition,String companyType){
    	if(isDomNull(condition)){
    		return true;
    	}
    	else{
    		if(isDomNull(companyType)){
    			return false;
    		}
    		if(condition.indexOf(companyType.trim())>=0){
    			return true;
    		}
    	}
    	return false;
    }
    
    /**
     * 
     * 判断专家所在地是否符合
     * @return boolean
     */
    public static boolean isLocation(String condition,String location){
    	if(isDomNull(condition)){
    		return true;
    	}
    	else{
    		if(isDomNull(location)){
    			return false;
    		}
    		if(condition.indexOf(location.trim())>=0){
    			return true;
    		}
    	}
    	return false;
    }
    
    /**
     * 
     * 判断工作荣誉头衔是否符合
     * @return boolean
     */
    public static boolean isWorkHonor(String condition,String workHonor){
    	String delim = ",";
    	if(isDomNull(condition)||condition.equals("15")){
    		return true;
    	}
    	else{
    		if(isDomNull(workHonor)){
    			return false;
    		}
    		String[] workHonor_a=workHonor.split(delim);
    		for(int i=0;i<workHonor_a.length;i++){
    			if(condition.indexOf(workHonor_a[i].trim())>=0){
        			return true;
        		}
    		}
    	}
    	return false;
    }
    
    /**
     * 
     * 判断专家原计划标签是否符合
     * @return boolean
     */
    public static boolean isOriginalLabel(String condition,String label){
    	if(condition.equals("0")){
    		return true;
    	}
    	else{
    		if(isDomNull(label)){
    			return false;
    		}
    		if(label.indexOf(condition.trim())>=0){
    			return true;
    		}
    	}
    	return false;
    }
    
    /**
     * 
     * 判断关键词是否符合
     * @return boolean
     */
    public static boolean isKeyWord(String condition,String keyWord,boolean flag){
    	if(isDomNull(condition)){
    		return true;
    	}
    	else{
    		String delim=",";
    		if(isDomNull(keyWord)){
    			return false;
    		}
    		if(flag){
    			StringTokenizer st = new StringTokenizer(condition, delim);
    			while(st.hasMoreTokens()){
    				if(keyWord.indexOf(st.nextToken().trim())==-1){
    					return false;
    				}
    			}
    			return true;
    		}
    		else{
    			StringTokenizer st = new StringTokenizer(condition, delim);
    			while(st.hasMoreTokens()){
    				if(keyWord.indexOf(st.nextToken().trim())>=0){
    					return true;
    				}
    			}
    		}
    	}
    	return false;
    }
    
    /**
     * 
     * 判断评审经验是否符合
     * @return boolean
     */
    public static boolean isJudgeExp(boolean flag,String reviewExperience){
    	if(!flag){
    		return true;
    	}
    	if(!isDomNull(reviewExperience)){
    		return true;
    	}
    	return false;
    }
    
    /**
     * 
     * 判断专家状态是否符合
     * @return boolean
     */
    public static boolean isStatus(String status){
    	if(isDomNull(status)) return false;
    	int status_i=Integer.parseInt(status.trim());
    	if(status_i==7){
    		return false;
    	}
    	return true;
    }
    
    /**
     * 
     * 判断专家科研情况
     * @return boolean
     */
    public static boolean isScResearch(boolean yjjl_bool,String yjjl){
    	if(!yjjl_bool) return true;
    	if(!isDomNull(yjjl)) return true;
    	return false;
    }
    
    /**
     * 
     * 判断专家是否在回避的单位集合中
     * @return boolean
     */
    public static boolean isAvoidCompany(String company,Set avoidCompanySet){
    	if(avoidCompanySet.isEmpty()){
    		return true;
    	}
    	if(isDomNull(company)){
    		return false;
    	}
    	if(!avoidCompanySet.contains(company.trim())){
    		return true;
    	}
    	return false;
    }
    
    /**
     * 
     * 判断专家是否在回避的专家集合中
     * @return boolean
     */
    public static boolean isAvoidExpert(String expertName,Set avoidExpertSet){
    	if(avoidExpertSet.isEmpty()||!avoidExpertSet.contains(expertName.trim())){
    		return true;
    	}
    	return false;
    }
    
    /**
     * 
     * 模糊词匹配
     * @return boolean
     */
    public static boolean isFuzzyWord(String condition,String zjlwqk,String zlzz,String yjjl){
    	String delim=";";
    	if(isDomNull(condition)) return true;
    	StringTokenizer st = new StringTokenizer(condition, delim);
    	while(st.hasMoreTokens()){
    		String s = st.nextToken().trim();
    		if(!(zjlwqk.contains(s)&&zlzz.contains(s)&&yjjl.contains(s))){
    			return false;
    		}
    	}
    	return true;
    }
}
