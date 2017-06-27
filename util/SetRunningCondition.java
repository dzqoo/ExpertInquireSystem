package neu.expertInquireSystem.util;

import java.util.HashMap;
import java.util.Map;

import edu.neu.expert.mvc.pojo.Avoid;
//import neu.expertInquireSystem.dataManager.AvoidCondition;
import edu.neu.expert.mvc.pojo.Expert;
//import neu.expertInquireSystem.dataManager.ExpertCondition;

public class SetRunningCondition {
	Expert expertcondition1 = new Expert();
	Avoid avoidcondition1 = new Avoid();
	
	/**
	 * expertcondition1
	 */
	public Expert getExpertcondition1() {
		return expertcondition1;
	}
	public void setExpertcondition1() {
		Map<String,String> workType = new HashMap<String,String>();
		workType.put("1", "01");
		workType.put("2", "02");
		workType.put("3", "03");
		workType.put("4", "04");
		workType.put("5", "05");
		workType.put("6", "06");
		workType.put("7", "07");
		Map<String,String> jobTitle = new HashMap<String,String>();
		jobTitle.put("1", "01");
		jobTitle.put("2", "02");
		jobTitle.put("3", "03");
		jobTitle.put("4", "04");
		jobTitle.put("5", "05");
		jobTitle.put("6", "06");
		jobTitle.put("7", "07");
		Map<String,String> qualification = new HashMap<String,String>();
		qualification.put("1", "01");
		qualification.put("2", "02");
		qualification.put("3", "03");
		qualification.put("4", "04");
		qualification.put("5", "05");
		qualification.put("6", "06");
		qualification.put("7", "07");
		Map<String,String> companyScale = new HashMap<String,String>();
		companyScale.put("1", "AA");
		companyScale.put("2", "AB");
		Map<String,String> province = new HashMap<String,String>();
		//province.put("1", "");
		province.put("1", "01");
		province.put("2", "02");
		province.put("3", "03");
		province.put("4", "04");
		province.put("5", "05");
		province.put("6", "06");
		Map<String,String> rank = new HashMap<String,String>();
		rank.put("1", "01");
		rank.put("2", "02");
		rank.put("3", "03");
		rank.put("4", "04");
		rank.put("5", "05");
		rank.put("6", "06");
		rank.put("7", "07");
		
	    boolean scienCon = false,judExper = false, other = false;
	    String originLabel = "863计划";
	    String andOrDomain = "OR";
		
	    String keyAndOr = "OR"; 
	    String fuzzyKey = "null"; 
	    String key = "null";
	    
	    String avoidKey = "null";
	    String[][] domainDirect = {{"11"}};;
	    int experNum = 5;
	    
	    expertcondition1.setAndOrDomain(andOrDomain);
	    expertcondition1.setAvoidKey(avoidKey);
	    expertcondition1.setCompanyScale(companyScale);
	    expertcondition1.setCompanyScalee(companyScale);
	    expertcondition1.setDomainDirect(domainDirect);
	    expertcondition1.setExperNum(experNum);
	    expertcondition1.setFuzzyKey(fuzzyKey);
	    expertcondition1.setJobTitle(jobTitle);
	    expertcondition1.setJudExper(judExper);
	    expertcondition1.setKey(key);
	    expertcondition1.setKeyAndOr(keyAndOr);
	    expertcondition1.setOriginLabel(originLabel);
	    expertcondition1.setOther(other);
	    expertcondition1.setProvince(province);
	    expertcondition1.setQualification(qualification);
	    expertcondition1.setRank(rank);
	    expertcondition1.setScienCon(scienCon);
	    expertcondition1.setWorkType(workType);
		
//		String[][] dom_condition = {{"11"}};
//		String work_xz_condition="";
//		String zjzc_condition="";
//		String zjzyzg_condition="03";
//		String dwlx_condition="AA;AB";
//		String zjszdq_ss_condition="";
//		String work_honor_condition="";
//		String zjly_zk_condition = "";
//		String work_gjc_condition="";
//		String fuzzyWord_condition="null";
//		boolean gjc_flag = false;
//		boolean yjjl_flag = true;
//		expertcondition1.setDomainDirect(domainDirect);
//		expertcondition1.setAndOrDomain(andOrDomain);
//		expertcondition1.setFuzzyKey(fuzzyKey);
//		expertcondition1.setWork_xz_condition(work_xz_condition);
//		expertcondition1.setZjzc_condition(zjzc_condition);
//		expertcondition1.setZjzyzg_condition(zjzyzg_condition);
//		expertcondition1.setDwlx_condition(dwlx_condition);
//		expertcondition1.setZjszdq_ss_condition(zjszdq_ss_condition);
//		expertcondition1.setWork_honor_condition(work_honor_condition);
//		expertcondition1.setZjly_zk_condition(zjly_zk_condition);
//		expertcondition1.setWork_gjc_condition(work_gjc_condition);
//		expertcondition1.setCyjh_condition(true);
//		expertcondition1.setFuzzyWord_condition(fuzzyWord_condition);
//		expertcondition1.setGjc_flag(gjc_flag);
//		expertcondition1.setYjjl_flag(yjjl_flag);
	}
	
	
	/**
	 * avoidcondition1
	 */
	public Avoid getAvoidcondition1() {
		return avoidcondition1;
	}
	public void setAvoidcondition1() {
		boolean bearCompany = true,partakeCompany = false,bearPerson = true,partakePerson = false,sameCompany =false,teacherStudent = false,fiveYear=false,judgeExpert=false,
	    samePaper=false,sameAcheive=false,ssfabz=false,znbz=false,zpw=false;
	    String expertName="校凤;昝航行";
	    String companyName="金川有色金属公司劳动卫生职业病研究所";
		
	    avoidcondition1.setAvoidId("");
	    avoidcondition1.setBearCompany(bearCompany);
	    avoidcondition1.setBearPerson(bearPerson);
	    avoidcondition1.setCompanyName(companyName);
	    avoidcondition1.setExpertName(expertName);
	    avoidcondition1.setFiveYear(fiveYear);
	    avoidcondition1.setJudgeExpert(judgeExpert);
	    avoidcondition1.setPartakeCompany(partakeCompany);
	    avoidcondition1.setPartakePerson(partakePerson);
	    avoidcondition1.setSameAcheive(sameAcheive);
	    avoidcondition1.setSameCompany(sameCompany);
	    avoidcondition1.setSamePaper(samePaper);
	    avoidcondition1.setSsfabz(ssfabz);
	    avoidcondition1.setTeacherStudent(teacherStudent);
	    avoidcondition1.setZnbz(znbz);
	    avoidcondition1.setZpw(zpw);
	    
	    
	    
	    
	    
		
//		Set<String> dwSet = new HashSet<String>();
//		dwSet.add("金川有色金属公司劳动卫生职业病研究所");
//    	dwSet.add("上海机电工程科技情报研究所");
//    	Set<String> zjSet = new HashSet<String>();
//    	zjSet.add("校凤");
//    	zjSet.add("昝航行");
//    	avoidcondition1.setDwSet(dwSet);
//    	avoidcondition1.setZjSet(zjSet);
	}
}
