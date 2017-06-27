package neu.expertInquireSystem.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class DataSet implements Serializable{

  private volatile JavaRDD<Row> expertRDD;
  private volatile SQLContext sqlContext;
  
  public void loadData(){
	  long start = System.currentTimeMillis();
	  /**
	   * 启动SparkContext，将HBase数据加载到RDD并缓存在内存中
	   */
	  SparkConf sparkConf = new SparkConf().setAppName("professorSearcher").set("spark.executor.memory", "1g");
      JavaSparkContext sc = new JavaSparkContext(sparkConf);
      this.sqlContext = new SQLContext(sc);

      Configuration conf = HBaseConfiguration.create();
      conf.set("hbase.zookeeper.property.clientPort", "2181");
      conf.set("hbase.zookeeper.quorum", "hadoop02");
	  conf.set(TableInputFormat.INPUT_TABLE, "t_b_zjxx");

		JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);
		JavaRDD<Row> expertRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Row>() {
			@Override
			public Row call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
				String zid = Bytes.toString(immutableBytesWritableResultTuple2._2().getRow());
				String zjxm = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "zjxm".getBytes()));
				String zjzjhm = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "zjzjhm".getBytes()));
				String zjszdw = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "zjszdw".getBytes()));
				String work_xz = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "work_xz".getBytes()));
				String zjzc = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "zjzc".getBytes()));
				String zjzyzg = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "zjzyzg".getBytes()));
				String dwlx = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "dwlx".getBytes()));
				String zjszdq_ss = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "zjszdq_ss".getBytes()));
				String work_honor = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "work_honor".getBytes()));
				String zjly_zk = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "zjly_zk".getBytes()));
				String work_gjc = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "work_gjc".getBytes()));
				String cyjh = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "cyjh".getBytes()));
				String zjzt = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "zjzt".getBytes()));
				String zjds = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "zjds".getBytes()));
				String zjds_bs = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "zjds_bs".getBytes()));
				String zjxk = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "zjxk".getBytes()));
				String zjxk_jy = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "zjxk_jy".getBytes()));
				String zjxk_jj = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "zjxk_jj".getBytes()));
				String zjly = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "zjly".getBytes()));
				String zjlwqk = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "zjlwqk".getBytes()));
				String zlzz = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "zlzz".getBytes()));
				String yjjl = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue("zjxx".getBytes(), "yjjl".getBytes()));
				return RowFactory.create(zid, zjxm, zjzjhm, zjszdw, work_xz, zjzc, zjzyzg, dwlx, zjszdq_ss, work_honor, zjly_zk, work_gjc,
							cyjh, zjzt, zjds, zjds_bs, zjxk, zjxk_jy, zjxk_jj, zjly, zjlwqk, zlzz, yjjl);
			}
		});
		expertRDD.cache();
		long expertRDDcount = expertRDD.count();
		this.expertRDD = expertRDD;
		
		 //L_T_xmdw
        String xmdw_tableName = "L_T_xmdw";
		final String xmdw_Family = "xmdw_family";
        conf.set(TableInputFormat.INPUT_TABLE, xmdw_tableName);
        JavaPairRDD<ImmutableBytesWritable, Result> xmdwPairRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);
        JavaRDD<Row> xmdwRDD = xmdwPairRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Row>() {
			@Override
			public Row call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
				String xmid = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(xmdw_Family.getBytes(), "xmid".getBytes()));
				String srid = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(xmdw_Family.getBytes(), "srid".getBytes()));
				String dwmc = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(xmdw_Family.getBytes(), "dwmc".getBytes()));
				String sf = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(xmdw_Family.getBytes(), "sf".getBytes()));
				return RowFactory.create(xmid, srid, dwmc, sf);
			}
		});
        
        String xmdwSchemaStr = "xmid,srid,dwmc,sf";

		List<StructField> xmdwFields = new ArrayList<StructField>();
		for (String fieldName: xmdwSchemaStr.split(",")) {
			xmdwFields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType xmdwSchema = DataTypes.createStructType(xmdwFields);
		DataFrame xmdwDF = sqlContext.createDataFrame(xmdwRDD, xmdwSchema);
		xmdwDF.cache();
		xmdwDF.registerTempTable(xmdw_tableName);
		long xmdwDFcount = xmdwDF.count();
		
		//L_T_xmhz
		String xmhz_tableName = "L_T_xmhz";
		final String xmhz_Family = "xmhz_family";
		conf.set(TableInputFormat.INPUT_TABLE, "L_T_xmhz");
        JavaPairRDD<ImmutableBytesWritable, Result> xmhzPairRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);
        JavaRDD<Row> xmhzRDD = xmhzPairRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Row>() {
			@Override
			public Row call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
				String xmid = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(xmhz_Family.getBytes(), "xmid".getBytes()));
				String srid = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(xmhz_Family.getBytes(), "srid".getBytes()));
				String sf = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(xmhz_Family.getBytes(), "sf".getBytes()));
				String hzxm = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(xmhz_Family.getBytes(), "hzxm".getBytes()));
				return RowFactory.create(xmid, srid, sf, hzxm);
			}
		});
        
        String xmhzSchemaStr = "xmid,srid,sf,hzxm";

		List<StructField> xmhzFields = new ArrayList<StructField>();
		for (String fieldName: xmhzSchemaStr.split(",")) {
			xmhzFields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType xmhzSchema = DataTypes.createStructType(xmhzFields);
		DataFrame xmhzDF = sqlContext.createDataFrame(xmhzRDD, xmhzSchema);
		xmhzDF.cache();
		xmhzDF.registerTempTable(xmhz_tableName);
		long xmhzDFcount = xmhzDF.count();
		
		//L_T_hbdszj
		String hbdszj_tableName = "L_T_hbdszj";
		final String hbdszj_Family = "hbdszj_family";
		conf.set(TableInputFormat.INPUT_TABLE, hbdszj_tableName);
        JavaPairRDD<ImmutableBytesWritable, Result> hbdszjPairRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);
        JavaRDD<Row> hbdszjRDD = hbdszjPairRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Row>() {
			@Override
			public Row call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
				String xm = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(hbdszj_Family.getBytes(), "xm".getBytes()));
				String xmid = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(hbdszj_Family.getBytes(), "xmid".getBytes()));
				String srid = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(hbdszj_Family.getBytes(), "srid".getBytes()));
				String sf = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(hbdszj_Family.getBytes(), "sf".getBytes()));
				String ds = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(hbdszj_Family.getBytes(), "ds".getBytes()));
				String ds_bs = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(hbdszj_Family.getBytes(), "ds_bs".getBytes()));
				return RowFactory.create(xm, xmid, srid, sf, ds, ds_bs);
			}
		});
        
        String hbdszjSchemaStr = "xm,xmid,srid,sf,ds,ds_bs";

		List<StructField> hbdszjFields = new ArrayList<StructField>();
		for (String fieldName: hbdszjSchemaStr.split(",")) {
			hbdszjFields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType hbdszjSchema = DataTypes.createStructType(hbdszjFields);
		DataFrame hbdszjDF = sqlContext.createDataFrame(hbdszjRDD, hbdszjSchema);
		hbdszjDF.cache();
		hbdszjDF.registerTempTable(hbdszj_tableName);
		long hbdszjDFcount = hbdszjDF.count();
		
		//L_T_zjhz
		String zjhz_tableName = "L_T_zjhz";
		final String zjhz_Family = "zjhz_family";
		conf.set(TableInputFormat.INPUT_TABLE, zjhz_tableName);
        JavaPairRDD<ImmutableBytesWritable, Result> zjhzPairRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);
        JavaRDD<Row> zjhzRDD = zjhzPairRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Row>() {
			@Override
			public Row call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
				String xm = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(zjhz_Family.getBytes(), "xm".getBytes()));
				String xmid = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(zjhz_Family.getBytes(), "xmid".getBytes()));
				String srid = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(zjhz_Family.getBytes(), "srid".getBytes()));
				String sf = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(zjhz_Family.getBytes(), "sf".getBytes()));
				String hzxm = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(zjhz_Family.getBytes(), "hzxm".getBytes()));
				return RowFactory.create(xm, xmid, srid, sf, hzxm);
			}
		});
        
        String zjhzSchemaStr = "xm,xmid,srid,sf,hzxm";

		List<StructField> zjhzFields = new ArrayList<StructField>();
		for (String fieldName: zjhzSchemaStr.split(",")) {
			zjhzFields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType zjhzSchema = DataTypes.createStructType(zjhzFields);
		DataFrame zjhzDF = sqlContext.createDataFrame(zjhzRDD, zjhzSchema);
		zjhzDF.cache();
		zjhzDF.registerTempTable(zjhz_tableName);
		long zjhzDFcount = zjhzDF.count();
		
		//L_T_hbxszj
		String hbxszj_tableName = "L_T_hbxszj";
		final String hbxszj_Family = "hbxszj_family";
		conf.set(TableInputFormat.INPUT_TABLE, hbxszj_tableName);
        JavaPairRDD<ImmutableBytesWritable, Result> hbxszjPairRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);
        JavaRDD<Row> hbxszjRDD = hbxszjPairRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Row>() {
			@Override
			public Row call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
				String xm = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(hbxszj_Family.getBytes(), "xm".getBytes()));
				String xmid = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(hbxszj_Family.getBytes(), "xmid".getBytes()));
				String srid = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(hbxszj_Family.getBytes(), "srid".getBytes()));
				String sf = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(hbxszj_Family.getBytes(), "sf".getBytes()));
				String zjxm = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(hbxszj_Family.getBytes(), "zjxm".getBytes()));
				return RowFactory.create(xm, xmid, srid, sf, zjxm);
			}
		});
        
        String hbxszjSchemaStr = "xm,xmid,srid,sf,zjxm";

		List<StructField> hbxszjFields = new ArrayList<StructField>();
		for (String fieldName: hbxszjSchemaStr.split(",")) {
			hbxszjFields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType hbxszjSchema = DataTypes.createStructType(hbxszjFields);
		DataFrame hbxszjDF = sqlContext.createDataFrame(hbxszjRDD, hbxszjSchema);
		hbxszjDF.cache();
		hbxszjDF.registerTempTable(hbxszj_tableName);
		long hbxszjDFcount = hbxszjDF.count();
		
		//L_Z_zdhbzj
		String zdhbzj_tableName = "L_Z_zdhbzj";
		final String zdhbzj_Family = "zdhbzj_family";
		conf.set(TableInputFormat.INPUT_TABLE, zdhbzj_tableName);
        JavaPairRDD<ImmutableBytesWritable, Result> zdhbzjPairRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class);
        JavaRDD<Row> zdhbzjRDD = zdhbzjPairRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Row>() {
			@Override
			public Row call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
				String id = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(zdhbzj_Family.getBytes(), "id".getBytes()));
				String zjxm = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(zdhbzj_Family.getBytes(), "zjxm".getBytes()));
				String zjzjhm = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(zdhbzj_Family.getBytes(), "zjzjhm".getBytes()));
				String zjly_zjlb = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(zdhbzj_Family.getBytes(), "zjly_zjlb".getBytes()));
				String zjly_dwid = Bytes.toString(immutableBytesWritableResultTuple2._2().getValue(zdhbzj_Family.getBytes(), "zjly_dwid".getBytes()));
				return RowFactory.create(id, zjxm, zjzjhm, zjly_zjlb, zjly_dwid);
			}
		});
        
        String zdhbzjSchemaStr = "id,zjxm,zjzjhm,zjly_zjlb,zjly_dwid";

		List<StructField> zdhbzjFields = new ArrayList<StructField>();
		for (String fieldName: zdhbzjSchemaStr.split(",")) {
			zdhbzjFields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType zdhbzjSchema = DataTypes.createStructType(zdhbzjFields);
		DataFrame zdhbzjDF = sqlContext.createDataFrame(zdhbzjRDD, zdhbzjSchema);
		zdhbzjDF.cache();
		zdhbzjDF.registerTempTable(zdhbzj_tableName);
		long zdhbzjDFcount = zdhbzjDF.count();

		long stop = System.currentTimeMillis();
    	System.out.println("the dataset load time is: " + (stop-start) + " ms");
		System.out.println("Experts RDD Count:" + expertRDDcount);
		System.out.println("xmdwDF DataFrame Count:" + xmdwDFcount);
		System.out.println("xmhzDF DataFrame Count:" + xmhzDFcount);
		System.out.println("hbdszjDF DataFrame Count:" + hbdszjDFcount);
		System.out.println("zjhzDF DataFrame Count:" +zjhzDFcount);
		System.out.println("hbxszjDF DataFrame Count:" + hbxszjDFcount);
		System.out.println("zdhbzjDF DataFrame Count:" + zdhbzjDFcount);
  }
  
  public JavaRDD<Row> getDataSet() {
    return expertRDD;
  }
  public SQLContext getSQLContext() {
	    return sqlContext;
	  }
}
