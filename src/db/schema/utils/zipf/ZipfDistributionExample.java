package db.schema.utils.zipf;

import java.util.Arrays;

/**
 * Generates dome data following the given distribution, computes the
 * distribution of values and plots it to standard out.
 * 
 * @param args
 */
public class ZipfDistributionExample {

	/**
	 * A simple function to double-check, taken from http://oc-co.org/?p=76
	 * 
	 * @param max_val
	 * @return
	 */
	static double simpleZipfRandom(double max_val) {
		return Math.exp(Math.random() * Math.log(max_val + 1.0)) - 1.0;
	}

	public static void main(String[] args) {

		final int size = 10;
		final double theta = 0.2;
		final double h = 0.2; // 0.2 means 80-20 rule

		final int no = 30;
		final long seed = System.currentTimeMillis();
		ZipfDistributionFromGrayEtAl distribution = new ZipfDistributionFromGrayEtAl(size, theta, seed);

		int max = Integer.MIN_VALUE;
		int min = Integer.MAX_VALUE;
		int[] dataset = new int[no];
		for (int t = 0; t < no; t++) {
			
			// take next from distribution
			int data = distribution.nextInt();
			
			// double check 
			// data = (int)simpleZipfRandom(size);
			
			if (data > max) {
				max = data;
			}
			if (data < min) {
				min = data;
			}
			dataset[t] = data;
		}
		Arrays.sort(dataset);
		int old = dataset[0];
		int count = 1;
		int boundary = (int) (size * h);
		double boundaryCount = 0.0;
		for (int t = 1; t < no; t++) {
			if (dataset[t] != old) {
				System.out.println(old + "\t" + count / (double) no);
				// get probability for h*N:
				if (old < boundary) {
					boundaryCount += count;
				}
				old = dataset[t];
				count = 0;
			}
			count++;
		}
		System.out.println("boundary count:\t" + boundaryCount / (double) no + "\t1-h:\t" + (1 - h));
	}
}
