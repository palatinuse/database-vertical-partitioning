package core.utils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class ArrayUtils {

	public static BigInteger arrayToBigInt(int[] data){
		BigInteger dataBigInt = BigInteger.valueOf(0);
		for(int i=0;i<data.length;i++)
			dataBigInt = dataBigInt.setBit(data[i]);
		return dataBigInt;
	}
	
	public static int[] bigIntToArray(BigInteger data){
		List<Integer> dataList = new ArrayList<Integer>();
		for(int i=0;data.bitCount()>0;i++,data=data.shiftRight(1)){
			if(data.testBit(0))
				dataList.add(i);
		}
		
		int[] dataArr = new int[dataList.size()];
		for(int i=0;i<dataArr.length;i++)
			dataArr[i] = dataList.get(i);
		
		return dataArr;
	}
	
	public static int[] concatenate(int[] a1, int[] a2){
		int[] a3 = new int[a1.length+a2.length];
		for(int i=0;i<a1.length;i++)
			a3[i] = a1[i];
		for(int i=0;i<a2.length;i++)
			a3[a1.length+i] = a2[i];
		return a3;
	}
	
	public static int[] identityArray(int size, int value){
		int[] array = new int[size];
		for(int i=0; i<array.length; i++)
			array[i] = value;
			
		return array;
	}
	
	public static int[][] getSubArray(int[][] array, int rows){
		int[][] subArray = new int[rows][];
		for(int i=0; i<rows && i<array.length; i++)
			subArray[i] = array[i];
		
		return subArray;
	}
	
	public static int[] getSubArray(int[] array, int[] indexes){
		int[] subArray = new int[indexes.length];
		for(int i=0;i<subArray.length;i++){
			subArray[i] = array[indexes[i]];
		}
		return subArray;
	}

    /* My Java environment does not support this kind of overloading*/

    /*public static int[] toArrayInteger(List<Integer> list){
        int[] array = new int[list.size()];
        for(int i=0;i<list.size();i++)
            array[i] = list.get(i);
        return array;
    }

    public static int[][] toArrayList(List<int[]> list){
        int[][] array = new int[list.size()][];
        for(int i=0;i<list.size();i++)
            array[i] = list.get(i);
        return array;
    }*/

	public static int[] toArrayInteger(List<Integer> list){
		int[] array = new int[list.size()];
		for(int i=0;i<list.size();i++)
			array[i] = list.get(i);
		return array;
	}

    public static int[][] toArrayList(List<int[]> list){
        int[][] array = new int[list.size()][];
        for(int i=0;i<list.size();i++)
            array[i] = list.get(i);
        return array;
    }
	
	public static void printArray(int[][] array, String arrayLabel, String rowLabel, String columnLabel){
		System.out.println(arrayLabel);
		for(int i=0;i<array.length;i++){
			if(rowLabel!=null)
				System.out.print(rowLabel+(i+1)+": ");
			for(int j=0;j<array[i].length;j++){
				if(columnLabel!=null)
					System.out.print(columnLabel+",");
				System.out.print(array[i][j]+" ");
			}
			System.out.println();
		}
		System.out.println();
	}
	
	public static void printArray(int[] array, String arrayLabel, String columnLabel){
		System.out.println(arrayLabel);
		for(int i=0;i<array.length;i++){
			if(columnLabel!=null)
				System.out.print(columnLabel+",");
			System.out.print(array[i]+" ");
		}
		System.out.println();
	}
	
	public static int[] simpleArray(int size, int start, int step){
		int[] a = new int[size];
		for(int i=0;i<a.length;i++)
			a[i] = start + i*step;
		return a;
	}
	
	public static String arrayToString(int[] array, String separator){
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<array.length;i++){
			sb.append(array[i]);
			if(i < array.length-1)
				sb.append(separator);
		}
		return sb.toString();
	}
}
