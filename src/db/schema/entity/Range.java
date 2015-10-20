package db.schema.entity;

import db.schema.types.AttributeType;
import db.schema.utils.Pair;

// TODO check
public class Range {

	public Attribute a;
	private Pair<Integer,Integer> ridRange;
	public boolean isIndexed = false;
	
	public Range(Attribute a, int offset, int cardinality){
		this.a = a;
		ridRange = new Pair<Integer, Integer>(offset, offset+cardinality-1);
	}
	
	public Range(Range r, int relativeOffset, int cardinality){
		this(r.a, r.getRidRange().first+relativeOffset, cardinality);
	}

	public Pair<Integer,Integer> getRidRange() {
		return ridRange;
	}

	public void setRidRange(Pair<Integer,Integer> ridRange) {
		this.ridRange = ridRange;
	}

	public int count(){
		return (ridRange.second - ridRange.first +1);
	}
	
	public int intersectCount(Range r){
		if(!this.a.equals(r.a))
			return 0;
		
		Pair<Integer,Integer> ridRange = r.getRidRange();
		
		int low = Math.max(this.ridRange.first, ridRange.first);
		int high = Math.min(this.ridRange.second, ridRange.second);
		
		if(low <= high)
			return (high-low+1);
		else
			return 0;
	}
	
	public int unionCount(Range r){
		if(!this.a.equals(r.a))
			return 0;
		
		Pair<Integer,Integer> ridRange = r.getRidRange();
		
		int count1 = this.ridRange.second - this.ridRange.first + 1;
		int count2 = ridRange.second - ridRange.first + 1;
		
		int low = Math.max(this.ridRange.first, ridRange.first);
		int high = Math.min(this.ridRange.second, ridRange.second);
		
		if(low <= high)
			return (Math.max(this.ridRange.second, ridRange.second)-Math.min(this.ridRange.first, ridRange.first)+1);
		else
			return (count1+count2);
	}
	
	public int hashCode(){
		return a.name.hashCode() + ridRange.first.hashCode() + ridRange.second.hashCode(); 
	}
	
	public static Range emptyRange(){
		Attribute dummy = new Attribute("dummy", AttributeType.Integer());
		return new Range(dummy, 0, 0);
	}
}
