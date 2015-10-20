package db.schema;

import db.schema.entity.Attribute;
import db.schema.entity.Query;
import db.schema.entity.Range;
import db.schema.entity.Workload;
import db.schema.utils.zipf.ZipfDistributionFromGrayEtAl;

import java.util.List;

public class BenchmarkWorkloads {

    /*Begin Debugging Begin*/
    public static Workload tpchAll(List<Attribute> attributes, double scaleFactor){
        Workload w = new Workload(attributes, (long)(scaleFactor * 6000000), "ALL");
        w.addProjectionQuery("A1", 1, 19, 6, 16, 5, 4, 1, 9);
        w.addProjectionQuery("A2", 1, 13, 28, 12, 5, 15, 4, 16);
        w.addProjectionQuery("A3", 1, 4, 5, 7, 9);
        w.addProjectionQuery("A4", 1, 27, 14, 25, 24);
        w.addProjectionQuery("A5", 1, 3, 17, 8, 11, 9);
        w.addProjectionQuery("A6", 1, 23, 22, 20, 26);
        w.addProjectionQuery("A7", 1, 10, 7, 22, 21, 20, 11, 4);
        w.addProjectionQuery("A8", 1, 14, 30, 29);
        w.addProjectionQuery("A9", 1, 3, 14, 8, 30, 18);
        w.addProjectionQuery("A10", 1, 2, 0);

        return w;
    }
    /*End Debuging End*/

	public static Workload tpchCustomer(List<Attribute> attributes, double scaleFactor){
		Workload w = new Workload(attributes, (long)(scaleFactor * 150000), "CUSTOMER");
		w.addProjectionQueryWithFiltering("Q3", 1, new int[]{6}, 2.00E-01, 0, 6);
		w.addProjectionQuery("Q5", 1, 0, 3);
		w.addProjectionQuery("Q7", 1, 0, 3);
		w.addProjectionQuery("Q8", 1, 0, 3);
		w.addProjectionQuery("Q10", 1, 0, 1, 2, 3, 4, 5, 7);
		w.addProjectionQuery("Q13", 1, 0);
		w.addProjectionQuery("Q18", 1, 0, 1);
		w.addProjectionQueryWithFiltering("Q22", 1, new int[]{4, 5}, 2.55E-01, 0, 4, 5);
		
		return w;
	}
	
	public static Workload tpchLineitem(List<Attribute> attributes, double scaleFactor){
		Workload w = new Workload(attributes, (long)(scaleFactor * 6000000), "LINEITEM");
		w.addProjectionQueryWithFiltering("Q1", 1, new int[]{10}, 9.90E-01, 4, 5, 6, 7, 8, 9, 10);
		w.addProjectionQueryWithFiltering("Q3", 1, new int[]{10}, 5.35E-01, 0, 5, 6, 10);
		//w.addProjectionQuery("Q4", 1, 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15); // the SELECT * would be optimized away
        w.addProjectionQueryWithFiltering("Q4", 1, new int[]{11, 12}, 6.32E-01, 0, 11, 12);
		w.addProjectionQuery("Q5", 1, 0,2,5,6);
		w.addProjectionQueryWithFiltering("Q6", 1, new int[]{4, 6, 10}, 1.98E-02, 4, 5, 6, 10);
		w.addProjectionQueryWithFiltering("Q7", 1, new int[]{10}, 3.04E-01, 0, 2, 5, 6, 10);
		w.addProjectionQuery("Q8", 1, 0,1,2,5,6);
		w.addProjectionQuery("Q9", 1, 0,1,2,4,5,6);
		w.addProjectionQueryWithFiltering("Q10", 1, new int[]{8}, 2.47E-01, 0, 5, 6, 8);
		w.addProjectionQueryWithFiltering("Q12", 1, new int[]{10, 11, 12, 14}, 5.18E-03, 0, 10, 11, 12, 14);
		w.addProjectionQueryWithFiltering("Q14", 1, new int[]{10}, 1.25E-02, 1, 5, 6, 10);
		w.addProjectionQueryWithFiltering("Q15", 1, new int[]{10}, 3.83E-02, 2, 5, 6, 10);
		w.addProjectionQuery("Q17", 1, 1,4,5);
		w.addProjectionQuery("Q18", 1, 0,4);
		w.addProjectionQueryWithFiltering("Q19", 1, new int[]{4, 13, 14}, 2.00E-02, 1, 4, 5, 6, 13, 14);
		w.addProjectionQueryWithFiltering("Q20", 1, new int[]{10}, 1.52E-01, 1, 2, 4, 10);
		//w.addProjectionQuery("Q21", 1, 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15);  // the SELECT * would be optimized away
        w.addProjectionQuery("Q21", 1, 0,2,11,12);
		
		return w;
	}

    public static Workload tpchPart(List<Attribute> attributes, double scaleFactor) {
        Workload w = new Workload(attributes, (long)(scaleFactor * 200000), "PART");
        
        w.addProjectionQueryWithFiltering("Q2", 1, new int[]{4, 5}, 3.93E-03, 0, 2, 4, 5);
        w.addProjectionQueryWithFiltering("Q8", 1, new int[]{4}, 6.68E-03, 0, 4);
        w.addProjectionQueryWithFiltering("Q9", 1, new int[]{1}, 5.47E-02, 0, 1);
        w.addProjectionQuery("Q14", 1, 0,4);
        w.addProjectionQueryWithFiltering("Q16", 1, new int[]{3, 4, 5}, 1.49E-01, 0, 3, 4, 5);
        w.addProjectionQueryWithFiltering("Q17", 1, new int[]{3, 6}, 1.00E-03, 0, 3, 6);
        w.addProjectionQueryWithFiltering("Q19", 1, new int[]{3, 5, 6}, 2.37E-03, 0, 3, 5, 6);
        w.addProjectionQueryWithFiltering("Q20", 1, new int[]{1}, 1.09E-02, 0, 1);

        return w;
    }

    public static Workload tpchSupplier(List<Attribute> attributes, double scaleFactor) {
        Workload w = new Workload(attributes, (long)(scaleFactor * 10000), "SUPPLIER");
        
        w.addProjectionQuery("Q2", 1, 0,1,2,3,4,5,6);
        w.addProjectionQuery("Q5", 1, 0,3);
        w.addProjectionQuery("Q7", 1, 0,3);
        w.addProjectionQuery("Q8", 1, 0,3);
        w.addProjectionQuery("Q9", 1, 0,3);
        w.addProjectionQuery("Q11", 1, 0,3);
        w.addProjectionQuery("Q15", 1, 0,1,2,4);
        w.addProjectionQueryWithFiltering("Q16", 1, new int[]{6}, 5.60E-04, 0, 6);
        w.addProjectionQuery("Q20", 1, 0,1,2,3);
        w.addProjectionQuery("Q21", 1, 0,1,3);

        return w;
    }

    public static Workload tpchPartSupp(List<Attribute> attributes, double scaleFactor) {
        Workload w = new Workload(attributes, (long)(scaleFactor * 800000), "PARTSUPP");
        
        w.addProjectionQuery("Q2", 1, 0,1,3);
        w.addProjectionQuery("Q9", 1, 0,1,3);
        w.addProjectionQuery("Q11", 1, 0,1,2,3);
        w.addProjectionQueryWithFiltering("Q16", 1, new int[]{1}, 9.99E-01, 0, 1);
        w.addProjectionQueryWithFiltering("Q20", 1, new int[]{0}, 1.09E-02, 0, 1, 2);

        return w;
    }

    public static Workload tpchOrders(List<Attribute> attributes, double scaleFactor) {
        Workload w = new Workload(attributes, (long)(scaleFactor * 1500000), "ORDERS");

        w.addProjectionQueryWithFiltering("Q3", 1, new int[]{4}, 4.90E-01, 0, 1, 4, 7);
        w.addProjectionQueryWithFiltering("Q4", 1, new int[]{4}, 3.75E-02, 0, 4, 5);
        w.addProjectionQueryWithFiltering("Q5", 1, new int[]{4}, 1.52E-01, 0, 1, 4);
        w.addProjectionQuery("Q7", 1, 0,1);
        w.addProjectionQueryWithFiltering("Q8", 1, new int[]{4}, 3.04E-01, 0, 1, 4);
        w.addProjectionQuery("Q9", 1, 0,4);
        w.addProjectionQueryWithFiltering("Q10", 1, new int[]{4}, 3.82E-02, 0, 1, 4);
        w.addProjectionQuery("Q12", 1, 0,5);
        w.addProjectionQueryWithFiltering("Q13", 1, new int[]{8}, 9.89E-01, 0, 1, 8);
        w.addProjectionQueryWithFiltering("Q18", 1, new int[]{0}, 5.00E-06, 0, 1, 3, 4);
        w.addProjectionQueryWithFiltering("Q21", 1, new int[]{2}, 4.87E-01, 0, 2);
        w.addProjectionQuery("Q22", 1, 1);

        return w;
    }

    public static Workload tpchNation(List<Attribute> attributes) {
        Workload w = new Workload(attributes, 25, "NATION");

        w.addProjectionQuery("Q2", 1, 0,1,2);
        w.addProjectionQuery("Q5", 1, 0,1,2);
        w.addProjectionQueryWithFiltering("Q7", 1, new int[]{1}, 0.04, 0, 1);
        w.addProjectionQuery("Q8", 1, 0,1,2);
        w.addProjectionQuery("Q9", 1, 0,1);
        w.addProjectionQuery("Q10", 1, 0,1);
        w.addProjectionQueryWithFiltering("Q11", 1, new int[]{1}, 0.04, 0, 1);
        w.addProjectionQueryWithFiltering("Q20", 1, new int[]{1}, 0.04, 0, 1);
        w.addProjectionQueryWithFiltering("Q21", 1, new int[]{1}, 0.04, 0, 1);

        return w;
    }

    public static Workload tpchRegion(List<Attribute> attributes) {
        Workload w = new Workload(attributes, 5, "REGION");

        w.addProjectionQueryWithFiltering("Q2", 1, new int[]{1}, 0.2, 0, 1);
        w.addProjectionQueryWithFiltering("Q5", 1, new int[]{1}, 0.2, 0, 1);
        w.addProjectionQueryWithFiltering("Q8", 1, new int[]{1}, 0.2, 0, 1);

        return w;
    }

    /**********************************/
    /************** SSB ***************/
	
	public static Workload ssbLineOrder(List<Attribute> attributes, double scaleFactor){
		Workload w = new Workload(attributes, (long)(scaleFactor * 6000000), "LINEORDER");
		w.addProjectionQuery("Q1.1", 1, 5,8,9,11);
		w.addProjectionQuery("Q1.2", 1, 5,8,9,11);
		w.addProjectionQuery("Q1.3", 1, 5,8,9,11);
		w.addProjectionQuery("Q2.1", 1, 3,4,5,12);
		w.addProjectionQuery("Q2.2", 1, 3,4,5,12);
		w.addProjectionQuery("Q2.3", 1, 3,4,5,12);
		w.addProjectionQuery("Q3.1", 1, 2,4,5,12);
		w.addProjectionQuery("Q3.2", 1, 2,4,5,12);
		w.addProjectionQuery("Q3.3", 1, 2,4,5,12);
		w.addProjectionQuery("Q3.4", 1, 2,4,5,12);
		w.addProjectionQuery("Q4.1", 1, 2,3,4,5,12,13);
		w.addProjectionQuery("Q4.2", 1, 2,3,4,5,12,13);
		w.addProjectionQuery("Q4.3", 1, 2,3,4,5,12,13);
		
		return w;
	}

    public static Workload ssbCustomer(List<Attribute> attributes, double scaleFactor){
        Workload w = new Workload(attributes, (long)(scaleFactor * 30000), "CUSTOMER");
        w.addProjectionQuery("Q3.1", 1, 0,4,5);
        w.addProjectionQuery("Q3.2", 1, 0,3,4);
        w.addProjectionQuery("Q3.3", 1, 0,3);
        w.addProjectionQuery("Q3.4", 1, 0,3);
        w.addProjectionQuery("Q4.1", 1, 0,4);
        w.addProjectionQuery("Q4.2", 1, 0,5);
        w.addProjectionQuery("Q4.3", 1, 0,5);

        return w;
    }

    public static Workload ssbPart(List<Attribute> attributes, double scaleFactor){
        Workload w = new Workload(attributes, (long)(200000 * Math.floor(1+ Math.log(scaleFactor))), "PART");
        w.addProjectionQuery("Q2.1", 1, 0,3,4);
        w.addProjectionQuery("Q2.2", 1, 0,4);
        w.addProjectionQuery("Q2.3", 1, 0,4);
        w.addProjectionQuery("Q4.1", 1, 0,2);
        w.addProjectionQuery("Q4.2", 1, 0,2,3);
        w.addProjectionQuery("Q4.3", 1, 0,3,4);

        return w;
    }

    public static Workload ssbSupplier(List<Attribute> attributes, double scaleFactor){
        Workload w = new Workload(attributes, (long)(scaleFactor * 2000), "SUPPLIER");
        w.addProjectionQuery("Q2.1", 1, 0,5);
        w.addProjectionQuery("Q2.2", 1, 0,5);
        w.addProjectionQuery("Q2.3", 1, 0,5);
        w.addProjectionQuery("Q3.1", 1, 0,4,5);
        w.addProjectionQuery("Q3.2", 1, 0,3,4);
        w.addProjectionQuery("Q3.3", 1, 0,3);
        w.addProjectionQuery("Q3.4", 1, 0,3);
        w.addProjectionQuery("Q4.1", 1, 0,5);
        w.addProjectionQuery("Q4.2", 1, 0,4,5);
        w.addProjectionQuery("Q4.3", 1, 0,3,4);

        return w;
    }

    public static Workload ssbDate(List<Attribute> attributes, double scaleFactor){
        Workload w = new Workload(attributes, (long)(7 * 365), "DATE");
        w.addProjectionQuery("Q1.1", 1, 0,4);
        w.addProjectionQuery("Q1.2", 1, 0,5);
        w.addProjectionQuery("Q1.3", 1, 0,4,11);
        w.addProjectionQuery("Q2.1", 1, 0,4);
        w.addProjectionQuery("Q2.2", 1, 0,4);
        w.addProjectionQuery("Q2.3", 1, 0,4);
        w.addProjectionQuery("Q3.1", 1, 0,4);
        w.addProjectionQuery("Q3.2", 1, 0,4);
        w.addProjectionQuery("Q3.3", 1, 0,4);
        w.addProjectionQuery("Q3.4", 1, 0,4,6);
        w.addProjectionQuery("Q4.1", 1, 0,4);
        w.addProjectionQuery("Q4.2", 1, 0,4);
        w.addProjectionQuery("Q4.3", 1, 0,4);

        return w;
    }

    /**********************************/

    public static Workload sdssPhotoObj(List<Attribute> attributes, double scaleFactor){
		Workload w = new Workload(attributes, (long)(scaleFactor * 375 * 1000), "PHOTO_OBJ");		// _TODO: verify/fix
		w.addProjectionQuery("Q1", 1, 0,1,2,3); // _TODO incorrect
		w.addProjectionQuery("Q2", 1, 0,4,5);
		w.addProjectionQuery("Q3", 1, 0,2,3,6,7,8,9,10,11,12,17,18,19,20,21,22,23,28,29);
		w.addProjectionQuery("Q4", 1, 0,1,6,7,23,24,25,26,27,30,31,32,33,34,35,36,37,38);
		w.addProjectionQuery("Q5", 1, 2,3,37,39); // _TODO check for correctness
		w.addProjectionQuery("Q6", 1, 0,8,9,10,11,12,30,37,40,41,42,43,44);
		w.addProjectionQuery("Q7", 1, 0,2,3);
		w.addProjectionQuery("Q8", 1, 0,2,3);
		
		return w;
	}
	
	public static Workload millionSongs(List<Attribute> attributes) {
		Workload w = new Workload(attributes, 1 * 1000 * 1000, "MILLIONSONG");
		
		Range illinois = new Range(attributes.get(3), 0, 11294);
		Range chicago = new Range(illinois, 0, 8510);
//		Range year1970_90 = new Range(attributes.get(7), 0, 66575);
//		Range year1970_80 = new Range(year1970_90, 0, 24748);
		Range year1970_80 = new Range(attributes.get(7), 0, 24748);	// _TODO: check!
		Range year1974_75 = new Range(year1970_80, 0, 4964);		// _TODO: check!
		Range year1975 = new Range(year1974_75, 0, 2482);
		Range hotSongs = new Range(attributes.get(6), 0, 175521);
		Range veryHotSongs = new Range(hotSongs, 0, 34035);
		Range superHotSongs = new Range(veryHotSongs, 0, 1229);
		
		
		w.addRangeQuery("Q1", 1, chicago, 1, 2, 3);
		w.addRangeQuery("Q2", 1, illinois, 0, 3);
		w.addRangeQuery("Q3", 1, year1975, 0, 6, 7);
		w.addRangeQuery("Q4", 1, year1974_75, 1, 4, 7);
		w.addRangeQuery("Q5", 1, year1970_80, 5, 7);
		w.addRangeQuery("Q6", 1, superHotSongs, 0, 1, 6);
		w.addRangeQuery("Q7", 1, veryHotSongs, 6, 7);
		w.addRangeQuery("Q8", 1, hotSongs, 3, 6);
		
		return w;
	}
	
	public static Workload randomWorkload(List<Attribute> attributes, int numQueries, int numRows){
		Workload w = new Workload(attributes, numRows, "RANDOM");
		ZipfDistributionFromGrayEtAl usageMDist = new ZipfDistributionFromGrayEtAl(attributes.size(), 0.9, System.currentTimeMillis());
		for(int i=0;i<numQueries;i++){
			int[] refAttributes = new int[usageMDist.nextInt() + 1];
			for(int j=0; j<refAttributes.length; j++)
				refAttributes[j] = usageMDist.nextInt();
			w.addProjectionQuery("Q"+i, 1, refAttributes);
		}
		
		return w;
	}
	
	public static Workload partialWorkload(Workload workload, int numQueries){
		Workload w = new Workload(workload.attributes, workload.getNumberOfRows(), workload.tableName);
		w.addProjectionQueries(workload.queries.subList(0, numQueries));
		return w;
	}
	
	public static Workload customWorkload(List<Attribute> attributes, List<Query> queries, long numRows, String tableName){
		Workload w = new Workload(attributes, numRows, tableName);
//		Set<Integer> uniqueProjections = new HashSet<Integer>();
//		for(Query q: allTPCHQueries){
//			for(int p: q.getProjectedColumns())
//				uniqueProjections.add(p);
//		}
//		for(Query q: allTPCHQueries)
//			q.setNumAttributes(uniqueProjections.size());
		w.addProjectionQueries(queries);
		return w;
	}
}
