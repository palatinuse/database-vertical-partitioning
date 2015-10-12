package db.schema.utils;

import db.schema.entity.Attribute;

import java.util.ArrayList;
import java.util.List;

public class AttributeUtils {

	public static String[] getAttributeNames(List<Attribute> attributes){
		String[] names = new String[attributes.size()];
		for(int i=0;i<names.length;i++)
			names[i] = attributes.get(i).name;
		return names;
	}
	
	public static int[] getAttributeSizes(List<Attribute> attributes){
		int[] sizes = new int[attributes.size()];
		for(int i=0;i<sizes.length;i++)
			sizes[i] = attributes.get(i).type.getSize();
		return sizes;
	}
	
	public static List<Attribute> getAttributeSubset(List<Attribute> attributes, int[] ids){
		List<Attribute> subset = new ArrayList<Attribute>();
		for(int id: ids)
			subset.add(attributes.get(id));
		return subset;
	}
}
