package db.schema;

import core.config.DataConfig;
import db.schema.entity.Attribute;
import db.schema.entity.Query;
import db.schema.entity.Table;
import db.schema.types.AttributeType;
import db.schema.types.TableType;
import db.schema.utils.AttributeUtils;
import db.schema.utils.WorkloadUtils;
import db.schema.utils.zipf.ZipfDistributionFromGrayEtAl;
import gnu.trove.map.hash.TIntIntHashMap;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class BenchmarkTables {
	public static enum Type {TPC_H_CUSTOMER, TPC_H_LINEITEM, TPC_H_ORDERS, TPC_H_PART, TPC_H_PARTSUPP, TPC_H_SUPPLIER, TPC_H_NATION, TPC_H_REGION}
	
	public static class BenchmarkConfig {
		private String dataFileDir;
		private double scaleFactor;
		private TableType tableType;
		
		public BenchmarkConfig(String dataFileDir, double scaleFactor, TableType tableType) {
			this.dataFileDir = dataFileDir;
			this.scaleFactor = scaleFactor;
			this.tableType = tableType;
		}
		public String getDataFileDir() {
			return dataFileDir;
		}
		public void setDataFileDir(String dataFileDir) {
			this.dataFileDir = dataFileDir;
		}
		public double getScaleFactor() {
			return scaleFactor;
		}
		public void setScaleFactor(double scaleFactor) {
			this.scaleFactor = scaleFactor;
		}
		public TableType getTableType() {
			return tableType;
		}
		public void setTableType(TableType tableType) {
			this.tableType = tableType;
		}
	}

    /*Begin Debugging Begin*/

    public static Table tpchAll(BenchmarkConfig conf){

        List<Attribute> attributes = new ArrayList<Attribute>();


        attributes.add(new Attribute("l_OrderKey", AttributeType.Integer()));
        //attributes.add(new Attribute("l_PartKey", AttributeType.Integer()));
        //attributes.add(new Attribute("l_SuppKey", AttributeType.Integer()));
        //attributes.add(new Attribute("l_Linenumber", AttributeType.Integer()));
        attributes.add(new Attribute("l_Quantity", AttributeType.Real()));
        attributes.add(new Attribute("l_ExtendedPrice", AttributeType.Real()));
        attributes.add(new Attribute("l_Discount", AttributeType.Real()));
        //attributes.add(new Attribute("l_Tax", AttributeType.Real()));
        //attributes.add(new Attribute("l_ReturnFlag", AttributeType.Character(1)));
        //attributes.add(new Attribute("l_LineStatus", AttributeType.Character(1)));
        attributes.add(new Attribute("l_ShipDate", AttributeType.Date("yyyy-MM-dd")));
        attributes.add(new Attribute("l_CommitDate", AttributeType.Date("yyyy-MM-dd")));
        attributes.add(new Attribute("l_ReceiptDate", AttributeType.Date("yyyy-MM-dd")));
        attributes.add(new Attribute("l_ShipInstruct", AttributeType.Character(25)));
        attributes.add(new Attribute("l_ShipMode", AttributeType.Character(10)));
        //attributes.add(new Attribute("l_Comment", AttributeType.CharacterVarying(44)));

        //attributes.add(new Attribute("c_Name", AttributeType.CharacterVarying(25)));
        //attributes.add(new Attribute("c_Address", AttributeType.CharacterVarying(40)));
        //attributes.add(new Attribute("c_NationKey", AttributeType.Integer()));
        attributes.add(new Attribute("c_Phone", AttributeType.Character(15)));
        attributes.add(new Attribute("c_AcctBal", AttributeType.Real()));
        attributes.add(new Attribute("c_MktSegment", AttributeType.Character(10)));
        //attributes.add(new Attribute("c_Comment", AttributeType.CharacterVarying(117)));


        //attributes.add(new Attribute("p_PartKey", AttributeType.Integer()));
        //attributes.add(new Attribute("p_Name", AttributeType.CharacterVarying(55)));
        //attributes.add(new Attribute("p_Mfgr", AttributeType.Character(25)));
        attributes.add(new Attribute("p_Brand", AttributeType.Character(10)));
        attributes.add(new Attribute("p_Type", AttributeType.CharacterVarying(25)));
        attributes.add(new Attribute("p_Size", AttributeType.Integer()));
        attributes.add(new Attribute("p_Container", AttributeType.Character(10)));
        //attributes.add(new Attribute("p_RetialPrice", AttributeType.Real()));
        //attributes.add(new Attribute("p_Comment", AttributeType.CharacterVarying(23)));




        //attributes.add(new Attribute("s_SuppKey", AttributeType.Integer()));
        attributes.add(new Attribute("s_Name", AttributeType.Character(25)));
        attributes.add(new Attribute("s_Address", AttributeType.CharacterVarying(40)));
        //attributes.add(new Attribute("s_NationKey", AttributeType.Integer()));
        //attributes.add(new Attribute("s_Phone", AttributeType.Character(15)));
        //attributes.add(new Attribute("s_AcctBal", AttributeType.Real()));
        //attributes.add(new Attribute("s_Comment", AttributeType.CharacterVarying(101)));


        attributes.add(new Attribute("ps_PartKey", AttributeType.Integer()));
        attributes.add(new Attribute("ps_SuppKey", AttributeType.Integer()));
        attributes.add(new Attribute("ps_AvailQty", AttributeType.Integer()));
        attributes.add(new Attribute("ps_SupplyCost", AttributeType.Real()));
        //attributes.add(new Attribute("ps_Comment", AttributeType.CharacterVarying(199)));

        //attributes.add(new Attribute("o_OrderKey", AttributeType.Integer()));
        //attributes.add(new Attribute("o_CustKey", AttributeType.Integer()));
        attributes.add(new Attribute("o_OrderStatus", AttributeType.Character(1)));
        //attributes.add(new Attribute("o_TotalPrice", AttributeType.Real()));
        attributes.add(new Attribute("o_OrderDate", AttributeType.Date("yyyy-MM-dd")));
        attributes.add(new Attribute("o_OrderPriority", AttributeType.Character(15)));
        //attributes.add(new Attribute("o_Clerk", AttributeType.Character(15)));
        attributes.add(new Attribute("o_ShipPriority", AttributeType.Integer()));
        //attributes.add(new Attribute("o_Comment", AttributeType.CharacterVarying(79)));

        attributes.add(new Attribute("ns_NationKey", AttributeType.Integer()));
        attributes.add(new Attribute("ns_Name", AttributeType.Character(25)));

        attributes.add(new Attribute("nc_NationKey", AttributeType.Integer()));
        attributes.add(new Attribute("nc_Name", AttributeType.Character(25)));
        //attributes.add(new Attribute("ns_RegionKey", AttributeType.Integer()));
        //attributes.add(new Attribute("ns_Comment", AttributeType.CharacterVarying(152)));

        //attributes.add(new Attribute("rc_RegionKey", AttributeType.Integer()));
        attributes.add(new Attribute("rc_Name", AttributeType.Character(25)));
        //attributes.add(new Attribute("rc_Comment", AttributeType.CharacterVarying(152)));


        Table t = new Table("tpcall", conf.getTableType(), attributes);
        t.pk = "l_OrderKey,l_Linenumber";

        List<String> ordered_attrs = Arrays.asList("c_acctbal", "c_mktsegment", "c_phone", "l_commitdate", "l_discount", "l_extendedprice", "l_orderkey", "l_quantity", "l_receiptdate", "l_shipdate", "l_shipinstruct", "l_shipmode", "nc_name", "nc_nationkey", "ns_name", "ns_nationkey", "o_orderdate", "o_orderpriority", "o_orderstatus", "o_shippriority", "p_brand", "p_container", "p_size", "p_type", "ps_availqty", "ps_partkey", "ps_suppkey", "ps_supplycost", "rc_name", "s_address", "s_name");

        for (int i=0; i<attributes.size(); ++i) {
            int pos = ordered_attrs.indexOf(attributes.get(i).name.toLowerCase());
            while (i != pos) {
                Attribute tmp = attributes.get(pos);
                attributes.set(pos, attributes.get(i));
                attributes.set(i, tmp);
                pos = ordered_attrs.indexOf(attributes.get(i).name.toLowerCase());
            }
        }
        for (int i=0; i<attributes.size(); ++i) {
            System.out.println(attributes.get(i).name);
        }


        t.workload = BenchmarkWorkloads.tpchAll(attributes, conf.getScaleFactor());
        t.workload.dataFileName = conf.getDataFileDir() + "lineitem.tbl";


        System.out.println(t.workload.dataFileName);
        return t;
    }
    /*End Debugging End*/

	public static Table tpchCustomer(BenchmarkConfig conf){
		Attribute custKey = new Attribute("c_CustKey", AttributeType.Integer());
		custKey.primaryKey = true;
		
		Attribute name = new Attribute("c_Name", AttributeType.CharacterVarying(25));
		Attribute address = new Attribute("c_Address", AttributeType.CharacterVarying(40));
		Attribute nationKey = new Attribute("c_NationKey", AttributeType.Integer());
		Attribute phone = new Attribute("c_Phone", AttributeType.Character(15));
		Attribute acctBal = new Attribute("c_AcctBal", AttributeType.Real());
		Attribute mktSegment = new Attribute("c_MktSegment", AttributeType.Character(10));
		Attribute comment = new Attribute("c_Comment", AttributeType.CharacterVarying(117));
		
		List<Attribute> attributes = new ArrayList<Attribute>();
		attributes.add(custKey);
		attributes.add(name);
		attributes.add(address);
		attributes.add(nationKey);
		attributes.add(phone);
		attributes.add(acctBal);
		attributes.add(mktSegment);
		attributes.add(comment);
		
		Table t = new Table("customer", conf.getTableType(), attributes);
        t.pk = "c_CustKey";
		t.workload = BenchmarkWorkloads.tpchCustomer(attributes, conf.getScaleFactor());
		t.workload.dataFileName = conf.getDataFileDir() + "customer.tbl";
		return t;
	}

	public static Table tpchLineitem(BenchmarkConfig conf){
		
		List<Attribute> attributes = new ArrayList<Attribute>();
		
		attributes.add(new Attribute("l_OrderKey", AttributeType.Integer()));
		attributes.add(new Attribute("l_PartKey", AttributeType.Integer()));
		attributes.add(new Attribute("l_SuppKey", AttributeType.Integer()));
		attributes.add(new Attribute("l_Linenumber", AttributeType.Integer()));
		attributes.add(new Attribute("l_Quantity", AttributeType.Real()));
		attributes.add(new Attribute("l_ExtendedPrice", AttributeType.Real()));
		attributes.add(new Attribute("l_Discount", AttributeType.Real()));
		attributes.add(new Attribute("l_Tax", AttributeType.Real()));
		attributes.add(new Attribute("l_ReturnFlag", AttributeType.Character(1)));
		attributes.add(new Attribute("l_LineStatus", AttributeType.Character(1)));
		attributes.add(new Attribute("l_ShipDate", AttributeType.Date("yyyy-MM-dd")));
		attributes.add(new Attribute("l_CommitDate", AttributeType.Date("yyyy-MM-dd")));
		attributes.add(new Attribute("l_ReceiptDate", AttributeType.Date("yyyy-MM-dd")));
		attributes.add(new Attribute("l_ShipInstruct", AttributeType.Character(25)));
		attributes.add(new Attribute("l_ShipMode", AttributeType.Character(10)));
		attributes.add(new Attribute("l_Comment", AttributeType.CharacterVarying(44)));
		
		Table t = new Table("lineitem", conf.getTableType(), attributes);
		t.pk = "l_OrderKey,l_Linenumber";
		t.workload = BenchmarkWorkloads.tpchLineitem(attributes, conf.getScaleFactor());
		t.workload.dataFileName = conf.getDataFileDir() + "lineitem.tbl";
		return t;
	}
	
	public static Table tpchPart(BenchmarkConfig conf){
		
		List<Attribute> attributes = new ArrayList<Attribute>();
		
		Attribute partKey = new Attribute("p_PartKey", AttributeType.Integer());
		partKey.primaryKey = true;
		attributes.add(partKey);
		attributes.add(new Attribute("p_Name", AttributeType.CharacterVarying(55)));
		attributes.add(new Attribute("p_Mfgr", AttributeType.Character(25)));
		attributes.add(new Attribute("p_Brand", AttributeType.Character(10)));
		attributes.add(new Attribute("p_Type", AttributeType.CharacterVarying(25)));
		attributes.add(new Attribute("p_Size", AttributeType.Integer()));
		attributes.add(new Attribute("p_Container", AttributeType.Character(10)));
		attributes.add(new Attribute("p_RetialPrice", AttributeType.Real()));
		attributes.add(new Attribute("p_Comment", AttributeType.CharacterVarying(23)));
		
		Table t = new Table("part", conf.getTableType(), attributes);
        t.pk = "p_PartKey";
		t.workload = BenchmarkWorkloads.tpchPart(attributes, conf.getScaleFactor());
		t.workload.dataFileName = conf.getDataFileDir() + "part.tbl";
		return t;
	}
	
	public static Table tpchSupplier(BenchmarkConfig conf){
		List<Attribute> attributes = new ArrayList<Attribute>();
		
		Attribute suppKey = new Attribute("s_SuppKey", AttributeType.Integer());
		suppKey.primaryKey = true;
		attributes.add(suppKey);
		attributes.add(new Attribute("s_Name", AttributeType.Character(25)));
		attributes.add(new Attribute("s_Address", AttributeType.CharacterVarying(40)));
		attributes.add(new Attribute("s_NationKey", AttributeType.Integer()));
		attributes.add(new Attribute("s_Phone", AttributeType.Character(15)));
		attributes.add(new Attribute("s_AcctBal", AttributeType.Real()));
		attributes.add(new Attribute("s_Comment", AttributeType.CharacterVarying(101)));
		
		Table t = new Table("supplier", conf.getTableType(), attributes);
		t.workload = BenchmarkWorkloads.tpchSupplier(attributes, conf.getScaleFactor());
		t.workload.dataFileName = conf.getDataFileDir() + "supplier.tbl";
		return t;
	}
	
	public static Table tpchPartSupp(BenchmarkConfig conf){
	
		List<Attribute> attributes = new ArrayList<Attribute>();
		
		attributes.add(new Attribute("ps_PartKey", AttributeType.Integer()));
		attributes.add(new Attribute("ps_SuppKey", AttributeType.Integer()));
		attributes.add(new Attribute("ps_AvailQty", AttributeType.Integer()));
		attributes.add(new Attribute("ps_SupplyCost", AttributeType.Real()));
		attributes.add(new Attribute("ps_Comment", AttributeType.CharacterVarying(199)));
		
		Table t = new Table("partsupp", conf.getTableType(), attributes);
		t.pk = "ps_PartKey,ps_SuppKey";
		t.workload = BenchmarkWorkloads.tpchPartSupp(attributes, conf.getScaleFactor());
		t.workload.dataFileName = conf.getDataFileDir() + "partsupp.tbl";
		return t;
	}
	
	public static Table tpchOrders(BenchmarkConfig conf){
		
		List<Attribute> attributes = new ArrayList<Attribute>();
		
		Attribute orderKey = new Attribute("o_OrderKey", AttributeType.Integer());
		orderKey.primaryKey = true;
		attributes.add(orderKey);
		attributes.add(new Attribute("o_CustKey", AttributeType.Integer()));
		attributes.add(new Attribute("o_OrderStatus", AttributeType.Character(1)));
		attributes.add(new Attribute("o_TotalPrice", AttributeType.Real()));
		attributes.add(new Attribute("o_OrderDate", AttributeType.Date("yyyy-MM-dd")));
		attributes.add(new Attribute("o_OrderPriority", AttributeType.Character(15)));
		attributes.add(new Attribute("o_Clerk", AttributeType.Character(15)));
		attributes.add(new Attribute("o_ShipPriority", AttributeType.Integer()));
		attributes.add(new Attribute("o_Comment", AttributeType.CharacterVarying(79)));
		
		Table t = new Table("orders", conf.getTableType(), attributes);
        t.pk = "o_OrderKey";
		t.workload = BenchmarkWorkloads.tpchOrders(attributes, conf.getScaleFactor());
		t.workload.dataFileName = conf.getDataFileDir() + "orders.tbl";
		return t;
	}
	
	public static Table tpchNation(BenchmarkConfig conf){
		
		List<Attribute> attributes = new ArrayList<Attribute>();
		
		Attribute nationKey = new Attribute("n_NationKey", AttributeType.Integer());
		nationKey.primaryKey = true;
		attributes.add(nationKey);
		attributes.add(new Attribute("n_Name", AttributeType.Character(25)));
		attributes.add(new Attribute("n_RegionKey", AttributeType.Integer()));
		attributes.add(new Attribute("n_Comment", AttributeType.CharacterVarying(152)));
		
		Table t = new Table("nation", conf.getTableType(), attributes);
        t.pk = "n_NationKey";
		t.workload = BenchmarkWorkloads.tpchNation(attributes);
		t.workload.dataFileName = conf.getDataFileDir() + "nation.tbl";
		return t;
	}
	
	public static Table tpchRegion(BenchmarkConfig conf){
		
		List<Attribute> attributes = new ArrayList<Attribute>();
		
		Attribute regionKey = new Attribute("r_RegionKey", AttributeType.Integer());
		regionKey.primaryKey = true;
		attributes.add(regionKey);
		attributes.add(new Attribute("r_Name", AttributeType.Character(25)));
		attributes.add(new Attribute("r_Comment", AttributeType.CharacterVarying(152)));
		
		Table t = new Table("region", conf.getTableType(), attributes);
        t.pk = "r_RegionKey";
		t.workload = BenchmarkWorkloads.tpchRegion(attributes);
		t.workload.dataFileName = conf.getDataFileDir() + "region.tbl";
		return t;
	}

    /**********************************/
    /************** SSB ***************/
	
	public static Table ssbLineOrder(BenchmarkConfig conf){
		
		List<Attribute> attributes = new ArrayList<Attribute>();
		
		attributes.add(new Attribute("lo_OrderKey", AttributeType.Integer()));
		attributes.add(new Attribute("lo_Linenumber", AttributeType.Integer()));
		attributes.add(new Attribute("lo_CustKey", AttributeType.Integer()));
		attributes.add(new Attribute("lo_PartKey", AttributeType.Integer()));
		attributes.add(new Attribute("lo_SuppKey", AttributeType.Integer()));
		attributes.add(new Attribute("lo_OrderDate", AttributeType.Integer()));
		attributes.add(new Attribute("lo_OrderPriority", AttributeType.Character(15)));
		attributes.add(new Attribute("lo_ShipPriority", AttributeType.Character(1)));
		attributes.add(new Attribute("lo_Quantity", AttributeType.Real()));
		attributes.add(new Attribute("lo_ExtendedPrice", AttributeType.Real()));
		attributes.add(new Attribute("lo_OrdTotalPrice", AttributeType.Real()));
		attributes.add(new Attribute("lo_Discount", AttributeType.Real()));
		attributes.add(new Attribute("lo_Revenue", AttributeType.Real()));
		attributes.add(new Attribute("lo_SuplyCost", AttributeType.Real()));
		attributes.add(new Attribute("lo_Tax", AttributeType.Real()));
		attributes.add(new Attribute("lo_CommitDate", AttributeType.Integer()));
		attributes.add(new Attribute("lo_ShipMode", AttributeType.Character(10)));
		
		Table t = new Table("lineorder", conf.getTableType(), attributes);
		t.pk = "lo_OrderKey,lo_Linenumber";
		t.workload = BenchmarkWorkloads.ssbLineOrder(attributes, conf.getScaleFactor());
		t.workload.dataFileName = conf.getDataFileDir() + "lineorder.tbl";
		
		return t;
	}

    public static Table ssbCustomer(BenchmarkConfig conf){

        Attribute custKey = new Attribute("c_CustKey", AttributeType.Integer());
        custKey.primaryKey = true;

        List<Attribute> attributes = new ArrayList<Attribute>();
        attributes.add(custKey);

        attributes.add(new Attribute("c_Name", AttributeType.CharacterVarying(25)));
        attributes.add(new Attribute("c_Address", AttributeType.CharacterVarying(25)));
        attributes.add(new Attribute("c_City", AttributeType.Character(10)));
        attributes.add(new Attribute("c_Nation", AttributeType.Character(15)));
        attributes.add(new Attribute("c_Region", AttributeType.Character(12)));
        attributes.add(new Attribute("c_Phone", AttributeType.Character(15)));
        attributes.add(new Attribute("c_MktSegment", AttributeType.Character(10)));

        Table t = new Table("customer", conf.getTableType(), attributes);
        t.pk = "c_CustKey";
        t.workload = BenchmarkWorkloads.ssbCustomer(attributes, conf.getScaleFactor());
        t.workload.dataFileName = conf.getDataFileDir() + "customer.tbl";
        return t;
    }

    public static Table ssbPart(BenchmarkConfig conf){

        List<Attribute> attributes = new ArrayList<Attribute>();

        Attribute partKey = new Attribute("p_PartKey", AttributeType.Integer());
        partKey.primaryKey = true;
        attributes.add(partKey);
        attributes.add(new Attribute("p_Name", AttributeType.CharacterVarying(22)));
        attributes.add(new Attribute("p_Mfgr", AttributeType.Character(6)));
        attributes.add(new Attribute("p_Category", AttributeType.Character(7)));
        attributes.add(new Attribute("p_Brand1", AttributeType.Character(9)));
        attributes.add(new Attribute("p_Color", AttributeType.Character(11)));
        attributes.add(new Attribute("p_Type", AttributeType.CharacterVarying(25)));
        attributes.add(new Attribute("p_Size", AttributeType.Integer()));
        attributes.add(new Attribute("p_Container", AttributeType.Character(10)));

        Table t = new Table("part", conf.getTableType(), attributes);
        t.pk = "p_PartKey";
        t.workload = BenchmarkWorkloads.ssbPart(attributes, conf.getScaleFactor());
        t.workload.dataFileName = conf.getDataFileDir() + "part.tbl";
        return t;
    }

    public static Table ssbSupplier(BenchmarkConfig conf){
        List<Attribute> attributes = new ArrayList<Attribute>();

        Attribute suppKey = new Attribute("s_SuppKey", AttributeType.Integer());
        suppKey.primaryKey = true;
        attributes.add(suppKey);
        attributes.add(new Attribute("s_Name", AttributeType.Character(25)));
        attributes.add(new Attribute("s_Address", AttributeType.CharacterVarying(25)));
        attributes.add(new Attribute("s_City", AttributeType.Character(10)));
        attributes.add(new Attribute("s_Nation", AttributeType.Character(15)));
        attributes.add(new Attribute("s_Region", AttributeType.Character(12)));
        attributes.add(new Attribute("s_Phone", AttributeType.Character(15)));

        Table t = new Table("supplier", conf.getTableType(), attributes);
        t.pk = "s_SuppKey";
        t.workload = BenchmarkWorkloads.ssbSupplier(attributes, conf.getScaleFactor());
        t.workload.dataFileName = conf.getDataFileDir() + "supplier.tbl";
        return t;
    }

    public static Table ssbDate(BenchmarkConfig conf){
        List<Attribute> attributes = new ArrayList<Attribute>();

        Attribute dateKey = new Attribute("D_DATEKEY", AttributeType.Integer());
        dateKey.primaryKey = true;
        attributes.add(dateKey);

        attributes.add(new Attribute("D_DATE", AttributeType.Character(18)));
        attributes.add(new Attribute("D_DAYOFWEEK", AttributeType.Character(8)));
        attributes.add(new Attribute("D_MONTH", AttributeType.Character(9)));
        attributes.add(new Attribute("D_YEAR", AttributeType.Integer()));
        attributes.add(new Attribute("D_YEARMONTHNUM", AttributeType.Integer()));
        attributes.add(new Attribute("D_YEARMONTH", AttributeType.Character(7)));
        attributes.add(new Attribute("D_DAYNUMINWEEK", AttributeType.Integer()));
        attributes.add(new Attribute("D_DAYNUMINMONTH", AttributeType.Integer()));
        attributes.add(new Attribute("D_DAYNUMINYEAR", AttributeType.Integer()));
        attributes.add(new Attribute("D_MONTHNUMINYEAR", AttributeType.Integer()));
        attributes.add(new Attribute("D_WEEKNUMINYEAR", AttributeType.Integer()));
        attributes.add(new Attribute("D_SELLINGSEASON", AttributeType.Character(12)));
        attributes.add(new Attribute("D_LASTDAYINWEEKFL", AttributeType.Boolean()));
        attributes.add(new Attribute("D_LASTDAYINMONTHFL", AttributeType.Boolean()));
        attributes.add(new Attribute("D_HOLIDAYFL", AttributeType.Boolean()));
        attributes.add(new Attribute("D_WEEKDAYFL", AttributeType.Boolean()));

        Table t = new Table("date", conf.getTableType(), attributes);
        t.pk = "D_DATEKEY";
        t.workload = BenchmarkWorkloads.ssbDate(attributes, conf.getScaleFactor());
        t.workload.dataFileName = conf.getDataFileDir() + "date.tbl";
        return t;
    }

    /**********************************/
	
	public static Table sdssPhotoObj(BenchmarkConfig conf){
		List<Attribute> attributes = new ArrayList<Attribute>();
		
		for(int i=0;i<46;i++)
			attributes.add(new Attribute("attribute_"+i, AttributeType.Real()));
		
		Table t = new Table("photoobj", conf.getTableType(), attributes);
        t.pk = "attribute_0";
		t.workload = BenchmarkWorkloads.sdssPhotoObj(attributes, conf.getScaleFactor());
		t.workload.dataFileName = conf.getDataFileDir() + "photoobj.tbl";
		
		return t;
	}
	
	public static Table millionSongs(BenchmarkConfig conf){
		List<Attribute> attributes = new ArrayList<Attribute>();
		
		attributes.add(new Attribute("title", AttributeType.CharacterVarying(400)));
		attributes.add(new Attribute("artist", AttributeType.CharacterVarying(400)));
		attributes.add(new Attribute("artist_familiarity", AttributeType.Real()));
		attributes.add(new Attribute("artist_location", AttributeType.CharacterVarying(100)));
		attributes.add(new Attribute("danceability", AttributeType.Real()));
		attributes.add(new Attribute("loudness", AttributeType.Real()));
		attributes.add(new Attribute("song_hotness", AttributeType.Real()));
		attributes.add(new Attribute("year", AttributeType.Integer()));
		
		Table t = new Table("millionsongs", conf.getTableType(), attributes);
        t.pk = "title, artist"; // TODO check
		t.workload = BenchmarkWorkloads.millionSongs(attributes);
		return t;
	}
	
	
	public static Table randomTable(int numAttributes, int numQueries){
		// generate attribute sizes
		int[] candidateTypes = new int[]{Types.CHAR, Types.INTEGER, Types.BIGINT, Types.REAL, Types.DOUBLE, Types.VARCHAR};
		ZipfDistributionFromGrayEtAl attSizeDist = new ZipfDistributionFromGrayEtAl(candidateTypes.length, 0.9, System.currentTimeMillis());
		List<Attribute> attributes = new ArrayList<Attribute>();
		for(int i=0; i<numAttributes; i++){
			switch(attSizeDist.nextInt()){
			case 0:		attributes.add(new Attribute("attribute_"+i, AttributeType.Character(25)));break;
			case 1:		attributes.add(new Attribute("attribute_"+i, AttributeType.Integer()));break;
			case 2:		attributes.add(new Attribute("attribute_"+i, AttributeType.BigInt()));break;
			case 3:		attributes.add(new Attribute("attribute_"+i, AttributeType.Real()));break;
			case 4:		attributes.add(new Attribute("attribute_"+i, AttributeType.Double()));break;
			case 5:		attributes.add(new Attribute("attribute_"+i, AttributeType.CharacterVarying(60)));break;
			}
		}
		Table t = new Table("random_"+numAttributes+"_"+numQueries, DataConfig.tableType, attributes);
        t.pk = "attribute_0";
		t.workload = BenchmarkWorkloads.randomWorkload(attributes, numQueries, 10*1000*1000);
		return t;
	}
	
	/**
	 * Method for creating a table from an existing one using only a subset of its attributes
	 * and also setting a workload that is only a subset of its workload.
	 * @param t The original table.
	 * @param attributes The attributes to select for the new table.
	 * @param queries The queries to select for the new tables workload.
	 * @return The new table.
	 */
	public static Table partialTable(Table t, int[] attributes, int[] queries){
		List<Attribute> partialAttributes = t.attributes;
		TIntIntHashMap attributeIdx = null;
		if(attributes != null){
			partialAttributes = AttributeUtils.getAttributeSubset(t.attributes, attributes);
			attributeIdx = new TIntIntHashMap();
			Arrays.sort(attributes);
			for(int i=0; i<attributes.length; i++)
				attributeIdx.put(attributes[i], i);
		}
		Table partialT = new Table(t.name, t.type, partialAttributes);
		
		List<Query> partialQueries = t.workload.queries;
		if(queries != null)
			partialQueries = WorkloadUtils.getQuerySubset(t.workload.queries, queries, attributeIdx);

        partialT.pk = t.pk;
		partialT.workload = BenchmarkWorkloads.customWorkload(partialAttributes, partialQueries,
                t.workload.getNumberOfRows(), t.name);
		partialT.workload.dataFileName = t.workload.dataFileName;	// _TODO: fix?
		
		return partialT;
	}

    /**
     * Method for creating a table from an existing one using only a subset of its attributes
     * and also setting a workload that is only a subset of its workload.
     * @param t The original table.
     * @param attributes The attributes to select for the new table.
     * @param queries The list of query names to select for the new tables workload.
     * @return The new table.
     */
    public static Table partialTable(Table t, int[] attributes, String[] queries){
        List<Attribute> partialAttributes = t.attributes;
        TIntIntHashMap attributeIdx = null;
        if(attributes != null){
            partialAttributes = AttributeUtils.getAttributeSubset(t.attributes, attributes);
            attributeIdx = new TIntIntHashMap();
            Arrays.sort(attributes);
            for(int i=0; i<attributes.length; i++)
                attributeIdx.put(attributes[i], i);
        }
        Table partialT = new Table(t.name, t.type, partialAttributes);

        List<Query> partialQueries = t.workload.queries;
        if(queries != null)
            partialQueries = WorkloadUtils.getQuerySubset(t.workload.queries, queries, attributeIdx);

        partialT.pk = t.pk;
        partialT.workload = BenchmarkWorkloads.customWorkload(partialAttributes, partialQueries,
                t.workload.getNumberOfRows(), t.name);
        partialT.workload.dataFileName = t.workload.dataFileName;	// _TODO: fix?

        return partialT;
    }
	
	/**
	 * Method for generating a table containing only integer attributes with names A, B, C, D... 
	 */
	public static Table simpleTable(TableType type, int numAttrs){
		List<Attribute> attributes = new ArrayList<Attribute>();
		Attribute a = new Attribute("A", AttributeType.Integer());
		a.primaryKey = true;
		attributes.add(a);
		
		for(int i=1;i<numAttrs;i++){
			attributes.add(new Attribute(Character.toString((char)(65+i)), AttributeType.Integer()));
		}
		//attributes.add(new Attribute("B", AttributeType.Integer()));
		//attributes.add(new Attribute("C", AttributeType.Integer()));
		//attributes.add(new Attribute("D", AttributeType.Integer()));
		
		Table t = new Table("sample", type, attributes);
		t.pk = "A";
		return t;
	}
	
	public static void generateSimpleTable(String filename, String delimiter, int size, int numAttrs){
		
		Random r = new Random(System.currentTimeMillis());
		int x = 1;
		int x_total = x;
		int curr_val = 1;
		
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			for(int i=1;i<=size;i++){
				StringBuffer buff = new StringBuffer();
				
				// pk
				buff.append(i);
				buff.append(delimiter);
				
				// selectivity attribute
				if(i <= x_total){
					buff.append(curr_val);
					buff.append(delimiter);
				}
				else{
					if ((double)x/size < 0.1){
						if((double)x/size < 0.01)
							x *= 10;
						else
							x *= 2;
						x_total += x;
						curr_val++;
						buff.append(curr_val);
						buff.append(delimiter);
					}
					else{
						buff.append(r.nextInt());
						buff.append(delimiter);
					}
				}
				
				// remaining attributes
				for(int j=2;j<numAttrs;j++){
					buff.append(r.nextInt());
					buff.append(delimiter);
				}
				buff.deleteCharAt(buff.length()-1);
				
				//buff.append(r.nextInt());
			
				// write to file
				writer.write(buff.toString());
				if(i < size)
					writer.newLine();
				
				if(i%100000 == 0)
					System.out.println(i);
			}
			
			writer.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
