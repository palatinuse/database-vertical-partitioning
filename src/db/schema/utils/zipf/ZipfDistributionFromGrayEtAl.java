package db.schema.utils.zipf;

/**
 * Generates a Zipf distribution according to the code from:
 * <p>
 * Gray, Sundaresan, Englert, Baclawski, Weinberger. "Quickly Generating
 * Billion-Record Synthetic Databases", SIGMOD 1994.
 * <p>
 * Integer k gets weight proportional to (1/k)^theta, 0 &lt theta &lt 1.
 * 
 * @author marcos
 */
public class ZipfDistributionFromGrayEtAl implements IntegerFactory {

	private MersenneTwisterFast randomEngine = null;

	private int maxRandomValue;

	private double theta;

	private double zetaN;

	private double zetaTwo;

	/**
	 * Instantiates a new ZipfDistributionFromGrayEtAl.
	 * 
	 * @param maxRandomValue
	 * @param theta - the skew, 0 &lt theta &lt 1.
	 * @param seed
	 */
	public ZipfDistributionFromGrayEtAl(int maxRandomValue, double theta, long seed) {
		this.maxRandomValue = maxRandomValue;
		this.randomEngine = new MersenneTwisterFast(seed);
		this.theta = theta;

		//if (theta <= 0 || theta >= 1) {
		if (theta <= 0) {
			throw new RuntimeException("invalid theta " + theta + " - should be between 0 and 1 (both exclusive)");
		}

		this.zetaN = zeta(maxRandomValue, theta);
		this.zetaTwo = zeta(2, theta);
	}

	private double zeta(int n, double theta) {
		double ans = 0D;
		for (int i = 1; i <= n; i++) {
			ans += Math.pow(1D / i, theta);
		}
		return ans;
	}

	/**
	 * Generates the next int in the interval [0, maxRandomValue).
	 */
	public int nextInt() {
		double alpha = 1D / (1D - theta);
		double eta = (1D - Math.pow(2D / maxRandomValue, 1D - theta)) / (1D - zetaTwo / zetaN);
		double u = randomEngine.nextDouble();
		double uz = u * zetaN;

		if (uz < 1D) {
			return 0;
		}
		if (uz < 1D + Math.pow(0.5D, theta)) {
			return 1;
		}
		return (int) (maxRandomValue * Math.pow(eta * u - eta + 1D, alpha));
	}

	public int getSize() {
		return maxRandomValue;
	}

	public static void main(String[] args) {
		ZipfDistributionFromGrayEtAl zipf = new ZipfDistributionFromGrayEtAl(1, 1.1, System.currentTimeMillis());
		for(int i=0;i<1000;i++)
			System.out.println(zipf.nextInt());
	}
}