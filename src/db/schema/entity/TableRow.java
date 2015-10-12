package db.schema.entity;



public class TableRow {

	protected Table t;
	protected Object[] values;
	private int valueCount;
	
	public TableRow(Table t){
		this.t = t;
		reset();
	}
	
	public void add(Object obj){
		values[valueCount] = obj;
		valueCount++;
	}
	
	public void set(int index, Object obj){
		values[index] = obj;
		valueCount++;
	}

	public void reset(){
		values = new Object[t.attributes.size()];
		for(int i=0;i<values.length;i++)
			values[i] = new NOVALUE();
		valueCount = 0;
	}
	
	public Object[] getValues(){
		return values;
	}
	
	public Table getTable(){
		return t;
	}
	
	public static class NOVALUE{
	}
}
