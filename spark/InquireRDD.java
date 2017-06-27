
package neu.expertInquireSystem.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;
import neu.expertInquireSystem.avoidLogic.ExpertAvoidLogic;
import edu.neu.expert.mvc.pojo.Requirement;
import edu.neu.expert.mvc.pojo.Expert;
import edu.neu.expert.mvc.pojo.Avoid;
//import neu.expertInquireSystem.dataManager.AvoidCondition;
//import neu.expertInquireSystem.dataManager.ExpertCondition;
//import neu.expertInquireSystem.dataManager.ExpertList;
//import neu.expertInquireSystem.dataManager.InquireCondition;
//import neu.expertInquireSystem.rmi.Server;

public class InquireRDD implements Serializable{
	
	private Requirement inquireCondition;
	private final Expert expertCondition;
	private final Avoid avoidCondition;
	private final int resultNum;
	private DataSet dataSet;
	
	public InquireRDD(Requirement inquireCondition,DataSet dataSet){
	  this.inquireCondition = inquireCondition;
	  this.expertCondition = inquireCondition.getExpectCondition();
	  this.avoidCondition = inquireCondition.getAvoidCondition();
//	  this.avoidCondition = inquireCondition.getAvoidCondition();
//	  this.resultNum = inquireCondition.getResultNum();
	  this.resultNum = inquireCondition.getExpectCondition().getExperNum();
	  this.dataSet = dataSet;
	}
	
	//将传过来的AND和Or 转化为true或false
	public static boolean isAndOr(String s){
		if(s.trim().equalsIgnoreCase("And")){
			return true;
		}
		return false;
		
	}
	
	//将collection 转化为String
	public static String collectionToString(Collection<String> c){
		StringBuilder sb = new StringBuilder();
		Iterator<String> it = c.iterator();
		while(it.hasNext()){
			sb.append(it.next().trim());
			sb.append(";");
		}
		return sb.toString();
	}
	
    public List<Map<String, String>> execute(){
    	long start = System.currentTimeMillis();
	
    	JavaRDD<Row> expertRDD = dataSet.getDataSet();//从内存中取得数据
    	SQLContext sqlContext = dataSet.getSQLContext();
    	/**
    	 * SparkSQL查询回避单位集合和回避专家集合
    	 */
    	//condition
    	String avoidId = avoidCondition.getAvoidId();   //项目组
    	boolean bearCompany = avoidCondition.isBearCompany();  //项目承担单位回避
        boolean partcompany = avoidCondition.isPartakeCompany();  //项目参与单位回避
        boolean bearperson = avoidCondition.isBearPerson();    //项目负责人回避
        boolean partperson = avoidCondition.isPartakePerson();   //项目参与人员回避
        boolean zpw = avoidCondition.isZpw();   //咨评委专家
        boolean znbz = avoidCondition.isZnbz();  //指南编制专家
        boolean ssfabz = avoidCondition.isSsfabz();   //实施方案编制专家
        boolean fiveyear = avoidCondition.isFiveYear(); //5年内共同参与的专家回避
        boolean isTeacherStudent = avoidCondition.isTeacherStudent();  //师生关系回避
        String avoidCompanys = avoidCondition.getCompanyName();  //回避专家String
        String avoidPersons = avoidCondition.getExpertName();   //回避单位String
        boolean avoidSameCompany = avoidCondition.isSameCompany();//回避同单位专家
        
        
        
        
		//project company avoid
		final Set<String> avoidCompanySet = new HashSet<String>();
		if (bearCompany || partcompany) {
            if (bearCompany && partcompany) {
                StringBuffer sb = new StringBuffer("select dwmc from L_T_xmdw");
                if(!avoidId.trim().equals("")){
                	sb.append(" where srid = ");
                	sb.append(avoidId.trim());
                }
                System.out.println("sb.toString()="+sb.toString());
                DataFrame results = sqlContext.sql(sb.toString());
        		Row[] re = results.collect();
        		for (Row row : re) {
        			avoidCompanySet.add(row.getString(0));
        		}
            }
            if (bearCompany && !partcompany) {
            	 StringBuffer sb = new StringBuffer("select dwmc from L_T_xmdw where sf = 1");
                 if(!avoidId.trim().equals("")){
                 	sb.append(" and srid = ");
                 	sb.append(avoidId.trim());
                 }
                 System.out.println("sb.toString()="+sb.toString());
                 DataFrame results = sqlContext.sql(sb.toString());
         	 	Row[] re = results.collect();
         		for (Row row : re) {
         			avoidCompanySet.add(row.getString(0));
         		}
            }
            if (!bearCompany && partcompany) {
            	StringBuffer sb = new StringBuffer("select dwmc from L_T_xmdw where sf = 2");
                if(!avoidId.trim().equals("")){
                	sb.append(" and srid = ");
                	sb.append(avoidId.trim());
                }
                System.out.println("sb.toString()="+sb.toString());
                DataFrame results = sqlContext.sql(sb.toString());
        	 	Row[] re = results.collect();
        		for (Row row : re) {
        			avoidCompanySet.add(row.getString(0));
        		}
            }
		}
		
		//project person avoid
		final Set<String> avoidPersonSet = new HashSet<String>();
		if (bearperson || partperson  || zpw || znbz || ssfabz || fiveyear) {
            if (bearperson && partperson) {
                StringBuffer sb = new StringBuffer("select xm from L_T_hbdszj");
                if(!avoidId.trim().equals("")){
                	sb.append(" where srid = ");
                	sb.append(avoidId.trim());
                }
                System.out.println("sb.toString()="+sb.toString());
                DataFrame results = sqlContext.sql(sb.toString());
        	 	Row[] re = results.collect();
        		for (Row row : re) {
        			avoidPersonSet.add(row.getString(0));
        		}
                
            }
            if (bearperson && !partperson) {
                StringBuffer sb = new StringBuffer("select xm from L_T_hbdszj where sf = 1");
                if(!avoidId.trim().equals("")){
                	sb.append(" and srid = ");
                	sb.append(avoidId.trim());
                }
                System.out.println("sb.toString()="+sb.toString());
                DataFrame results = sqlContext.sql(sb.toString());
        	 	Row[] re = results.collect();
        		for (Row row : re) {
        			avoidPersonSet.add(row.getString(0));
        		}
            }
            if (!bearperson && partperson) {
                StringBuffer sb = new StringBuffer("select xm from L_T_hbdszj where sf = 2");
                if(!avoidId.trim().equals("")){
                	sb.append(" and srid = ");
                	sb.append(avoidId.trim());
                }
                System.out.println("sb.toString()="+sb.toString());
                DataFrame results = sqlContext.sql(sb.toString());
        	 	Row[] re = results.collect();
        		for (Row row : re) {
        			avoidPersonSet.add(row.getString(0));
        		}
            }
            if (isTeacherStudent) {
            	if(avoidId.trim().equals("")){
            		System.out.println("avoidId.trim().equals()");
            		DataFrame results = sqlContext.sql("select ds from L_T_hbdszj");
            	 	Row[] re = results.collect();
            		for (Row row : re) {
            			avoidPersonSet.add(row.getString(0));
            		}
            		results = sqlContext.sql("select ds_bs from L_T_hbdszj");
            		re = results.collect();
            		for (Row row : re) {
            			avoidPersonSet.add(row.getString(0));
            		}
            		results = sqlContext.sql("select zjxm from L_T_hbxszj");
            		re = results.collect();
            		for (Row row : re) {
            			avoidPersonSet.add(row.getString(0));
            		}
            	}
            	else{
            		System.out.println("avoidId.trim().equals() flag");
            		DataFrame results = sqlContext.sql("select ds from L_T_hbdszj where srid = "+avoidId.trim());
            	 	Row[] re = results.collect();
            		for (Row row : re) {
            			avoidPersonSet.add(row.getString(0));
            		}
            		results = sqlContext.sql("select ds_bs from L_T_hbdszj where srid = "+avoidId.trim());
            		re = results.collect();
            		for (Row row : re) {
            			avoidPersonSet.add(row.getString(0));
            		}
            		results = sqlContext.sql("select zjxm from L_T_hbxszj where srid = "+avoidId.trim());
            		re = results.collect();
            		for (Row row : re) {
            			avoidPersonSet.add(row.getString(0));
            		}
            	}
            }
            if (zpw || znbz || ssfabz) {
                StringBuffer sb = new StringBuffer();
                if (zpw) {
                    sb.append("'zpw',");
                }
                if (znbz) {
                    sb.append("'znbz',");
                }
                if (ssfabz) {
                    sb.append("'ssfabz',");
                }
                sb = sb.deleteCharAt(sb.length() - 1);
                DataFrame results = sqlContext.sql("select zjxm from L_Z_zdhbzj  where zjly_zjlb not in ("+sb.toString()+")");
                Row[] re = results.collect();
        		for (Row row : re) {
        			avoidPersonSet.add(row.getString(0));
        		}
            }
            if (fiveyear) {
            	StringBuffer sb = new StringBuffer("select hzxm from L_T_zjhz");
            	if(!avoidId.trim().equals("")){
            		sb.append(" where srid = ");
            		sb.append(avoidId.trim());
            	}
            	DataFrame results = sqlContext.sql(sb.toString());
            	Row[] re = results.collect();
        		for (Row row : re) {
        			avoidPersonSet.add(row.getString(0));
        		}

            }
            
        }
		if (!avoidCompanys.trim().equals("")) {
            String[] avoidcompany = avoidCompanys.split("[,|；|，|\\|;]");
            for(String company : avoidcompany){
            	avoidCompanySet.add(company.trim());
            }
        }
        if (!avoidPersons.trim().equals("")) {
            String[] avoidPerson = avoidPersons.split("[,|；|，|\\|;]");
            for(String person : avoidPerson){
            	avoidPersonSet.add(person);
            }
        }
    	
    	/**
    	 * 根据查询条件执行过滤操作
    	 */
    	JavaRDD<Row> filterRDD = expertRDD.filter(new Function<Row, Boolean>() {
			@Override
			public Boolean call(Row row) throws Exception {
				String zid = row.getString(0);
				String zjxm = row.getString(1);
				String zjzjhm = row.getString(2);
				String zjszdw = row.getString(3);
				String work_xz = row.getString(4);
				String zjzc = row.getString(5);
				String zjzyzg = row.getString(6);
				String dwlx = row.getString(7);
				String zjszdq_ss = row.getString(8);
				String work_honor = row.getString(9);
				String zjly_zk = row.getString(10);
				String work_gjc = row.getString(11);
				String cyjh = row.getString(12);
				String zjzt = row.getString(13);
				String zjds = row.getString(14);
				String zjds_bs = row.getString(15);
				String zjxk = row.getString(16);
				String zjxk_jy = row.getString(17);
				String zjxk_jj = row.getString(18);
				String zjly = row.getString(19);
				String zjlwqk = row.getString(20);
				String zlzz = row.getString(21);
				String yjjl = row.getString(22);

				if(!ExpertAvoidLogic.isDomainDir(expertCondition.getDomainDirect(),InquireRDD.isAndOr(expertCondition.getAndOrDomain()),zjxk,zjxk_jy,zjxk_jj,zjly)){
//					.getDom_condition(),expertCondition.isDom_flag(),zjxk,zjxk_jy,zjxk_jj,zjly)){//专家领域
	            	return false;
	            }
//	            if(!ExpertAvoidLogic.isWorkProperty(expertCondition.getWork_xz_condition(), work_xz)){//工作性质
				if(!ExpertAvoidLogic.isWorkProperty(InquireRDD.collectionToString(expertCondition.getWorkType().values()),work_xz)){
						return false;
	            }
	            if(!ExpertAvoidLogic.isTitle(InquireRDD.collectionToString(expertCondition.getJobTitle().values()), zjzc)){//职称
	            	return false;
	            }
	            if(!ExpertAvoidLogic.isMIPA(InquireRDD.collectionToString(expertCondition.getQualification().values()),zjzyzg)){//执业资格
	            	return false;
	            }
	            if(!ExpertAvoidLogic.isCompanyType(InquireRDD.collectionToString(expertCondition.getCompanyScalee().values()),dwlx)){//单位类型
	            	return false;
	            }
	            if(!ExpertAvoidLogic.isLocation(InquireRDD.collectionToString(expertCondition.getProvince().values()),zjszdq_ss)){//所在地
	            	return false;
	            }
	            if(!ExpertAvoidLogic.isWorkHonor(InquireRDD.collectionToString(expertCondition.getRank().values()),work_honor)){//学术荣誉
	            	return false;
	            }
	            if(!ExpertAvoidLogic.isOriginalLabel(expertCondition.getOriginLabel(),zjly_zk)){//所属原计划
	            	return false;
	            }
	            if(!ExpertAvoidLogic.isJudgeExp(expertCondition.isJudExper(),cyjh)){//评审经验
	            	return false;
	            }
	        	if(!ExpertAvoidLogic.isStatus(zjzt)){//状态
	        		return false;
	        	}
	        	if(!ExpertAvoidLogic.isScResearch(expertCondition.isScienCon(),yjjl)){//科研情况
	        		return false;
	        	}
	        	if(!ExpertAvoidLogic.isAvoidCompany(zjszdw,avoidCompanySet)){//回避单位集合
	        		return false;
	        	}
	        	if(!ExpertAvoidLogic.isAvoidExpert(zjxm,avoidPersonSet)){//回避专家集合
	        		return false;
	        	}
	        	if(!ExpertAvoidLogic.isKeyWord(expertCondition.getKey(), work_gjc, InquireRDD.isAndOr(expertCondition.getKey()))){//关键词
	        		return false;
	        	}
	        	if(!ExpertAvoidLogic.isFuzzyWord(expertCondition.getFuzzyKey(),zjlwqk,zlzz,yjjl)){//模糊词
	        		return false;
	        	}
	        	return true;
			}
		});

    	/**
    	 * 对过滤后的专家集执行同单位回避操作（相同单位随机选一个）
    	 */
    	JavaRDD<Row> outRDD = null;
    	
    	if(avoidSameCompany){
    		JavaPairRDD<String, ArrayList<String>> avoidRDD = filterRDD.mapToPair(new PairFunction<Row, String, ArrayList<String>>() {
    			@Override
    			public Tuple2<String, ArrayList<String>> call(Row row) throws Exception {
    				String zid = row.getString(0);
    				String zjxm = row.getString(1);
    				String zjzc = row.getString(5);
    				String zjszdw = row.getString(3);
    				ArrayList<String> infor = new ArrayList<String>();
    				infor.add(zid);
    				infor.add(zjxm);
    				infor.add(zjzc);
    				infor.add(zjszdw);
    				return new Tuple2<String, ArrayList<String>>(zjszdw, infor);
    			}
    		});
    		JavaPairRDD<String, Iterable<ArrayList<String>>> groupRDD = avoidRDD.groupByKey();
    		outRDD = groupRDD.map(new Function<Tuple2<String, Iterable<ArrayList<String>>>, Row>() {
    			@Override
    			public Row call(Tuple2<String, Iterable<ArrayList<String>>> stringIterableTuple2) throws Exception {
    				Iterator<ArrayList<String>> it = stringIterableTuple2._2().iterator();
    				int length = 0;
    				while (it.hasNext()) {
    					it.next();
    					length++;
    				}
    				Random r = new Random();
    				int i = r.nextInt(length);
    				int length1 = 0;
    				ArrayList<String> al_String = null;
    				it = stringIterableTuple2._2().iterator();
    				while (it.hasNext()) {
    					if (length1 == i) {
    						al_String = it.next();
    						break;
    					}
    					it.next();
    					length1++;
    				}
    				return RowFactory.create(al_String.get(0),al_String.get(1),al_String.get(2),al_String.get(3));
    			}
    		});
    	}
    	else{
    		outRDD = filterRDD.map(new Function<Row, Row>() {
				@Override
    			public Row call(Row row) throws Exception {
    				String zid = row.getString(0);
    				String zjxm = row.getString(1);
    				String zjzc = row.getString(5);
    				String zjszdw = row.getString(3);
    				return RowFactory.create(zid,zjxm,zjzc,zjszdw);
    			}
			});
    	}
		
		long count = outRDD.count();
		
		/**
		 * 从查询结果集随机选择N条结果返回（N由用户指定）
		 */
		List<Row> rowList = null;
		if(resultNum < count){
			rowList = outRDD.takeSample(false, resultNum);
			System.out.println("the result count: " + count);
			System.out.println("return " + resultNum + " numbers of the result randomly");
		}else{
			rowList = outRDD.collect();
			System.out.println("the result count: " + count);
			System.out.println("return " + count + " numbers of the result randomly");
		}
		
		/**
		 * 将最终查询结果存储到InquireResult中，以便客户端取回结果
		 */
		List<Map<String, String>> expertList = new ArrayList<Map<String, String>>();
		for(Row row : rowList){
			Map<String,String> map = new HashMap<String,String>();
			map.put("zid", row.getString(0));
			map.put("zjxm", row.getString(1));
			map.put("zjzc", row.getString(2));
			map.put("zjszdw", row.getString(3));
			expertList.add(map);
		}
		
		long stop = System.currentTimeMillis();
    	System.out.println("the query runtime is: " + (stop-start) + " ms");
 
//		ExpertList expertList=new ExpertList();
//		expertList.setExpertList(stringList);
		return expertList;
//		Server.getInquireResult().put(inquireCondition, expertList);
  }
}
