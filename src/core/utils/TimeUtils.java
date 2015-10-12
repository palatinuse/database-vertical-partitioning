package core.utils;

public class TimeUtils {

	public static class Timer{
		private long startTime;
		private double elapsedTime; 
		
		public void start(){
			startTime = System.nanoTime();
		}
		
		public void stop(){
			elapsedTime += (double)(System.nanoTime() - startTime)/1E9;
			startTime = System.nanoTime();
		}
		
		public void reset(){
			elapsedTime = 0;
		}
		
		public double getElapsedTime(){
			return elapsedTime;
		}
	}
}
