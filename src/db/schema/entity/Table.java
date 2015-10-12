package db.schema.entity;

import db.schema.types.TableType;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.util.Arrays;
import java.util.List;



/**
 * Define a postgreSQL table here
 * 
 * @author alekh
 *
 */
public class Table {

	public String name;
	public TableType type;
	public List<Attribute> attributes;
	
	public List<Table> inherits = null;
	public String pk;
	
	public TIntObjectHashMap<TIntHashSet> partitions = null;
	
	public Workload workload;
	
	public Table(String name, TableType type, List<Attribute> attributes){
		this.name = name;
		this.type = type;
		this.attributes = attributes;
		for(Attribute a: attributes)
			if(a.primaryKey)
				pk = a.name;
	}
	
	public String getCSVAttributeList(){
		String str = "";
		for(int i=0;i<attributes.size();i++){
			str += attributes.get(i).name;
			if(i < attributes.size()-1)
				str += ",";
		}
		return str;
	}
	
	public String toString(){
		String tableStr = type.toString() + " " + name + "(";
		for(int i=0;i<attributes.size();i++){
			tableStr += attributes.get(i).toString();
			if(i<attributes.size()-1)
				tableStr += ",\n";
		}
		tableStr += "\n)";
		if(inherits!=null){
			tableStr += " INHERITS ( ";
			for(int i=0;i<inherits.size();i++){
				tableStr += inherits.get(i).name;
				if(i<inherits.size()-1)
					tableStr += ",";
			}
			tableStr += " )";
		}
		
		return tableStr;
	}
    
    public String toCTableString(TIntObjectHashMap<TIntHashSet> partitions) {

        StringBuilder sb = new StringBuilder();
        String partitionsString;

        for (TIntIterator pit = partitions.keySet().iterator(); pit.hasNext(); ) {
            TIntHashSet p = partitions.get(pit.next());

            int[] partition = p.toArray();
            Arrays.sort(partition);

            for (int i=0; i<partition.length - 1; i++) {
                sb.append(partition[i] + 1);
                sb.append(',');
            }
            sb.append(partition[partition.length - 1] + 1);

            if (pit.hasNext()) {
                sb.append('|');
            }
        }

        partitionsString = sb.toString();
        
        return "CREATE " + this.toString() 
                + " with (c_partitions=\"" + partitionsString
                + "\",c_buffer_size=250,c_segment_size=10000000);";
    }
	
	public boolean equals(Object obj){
		if(((Table)obj).name.equals(this.name))
			return true;
		else
			return false;
	}
}
