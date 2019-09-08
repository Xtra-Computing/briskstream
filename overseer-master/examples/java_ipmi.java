import ch.usi.overseer.OverIpmi;
import ch.usi.overseer.OverHpc;
import java.util.concurrent.atomic.AtomicInteger;

public class java_ipmi
{
	///////////
	// CONST //
	///////////	
	
	// Power consumption sensor:
	public static final int systemLevel = 13;
	
	// Sleep time:
	public static final int sleepTime = 3000;	

	// Fibonacci's stuff:
	public static final int depth = 50;		
	public static boolean INTERLEAVED = true;	
	public static AtomicInteger barrier = new AtomicInteger();
	


	//////////////////////
	// FIBONACCI THREAD //
	//////////////////////
	public static class FibonacciThread extends Thread
	{		
		// Methods:	
		public int fibo(int n) 
		{ 
			if ((n==0) || (n==1))
			{ 
				return n; 
			}
			else
			{
				return (fibo(n-1)+fibo(n-2)); 
			}
		}	
	
		FibonacciThread()
		{
			start();
		}
	
		public void run() 
		{
			// Set affinity:
			OverHpc oHpc = OverHpc.getInstance();
			int numberOfCpus = oHpc.getNumberOfCpus();
			int mask[] = new int[oHpc.getNumberOfPus()];
			
			if (INTERLEAVED)
			{			
				for (int cpu=0; cpu<numberOfCpus; cpu++)
					mask[oHpc.getCpuCoreMapping(cpu, 0)] = 1;										
			}
			else
			{
				for (int core=0; core<numberOfCpus; core++)
					mask[oHpc.getCpuCoreMapping(0, core)] = 1;
			}
			oHpc.setThreadAffinityMask(oHpc.getThreadId(), mask);
		
			// Do Fibonacci:		
			int c = fibo(depth);
			barrier.incrementAndGet();
		}
	}
		

	//////////
	// MAIN //
	//////////	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) 
	{
		// Credits:
        	System.out.println("Overseer Java IPMI test, A. Peternier (C) USI 2011\n");
		
		// Make sure we are running on (at least) a dual-CPU machine:
		OverHpc oHpc = OverHpc.getInstance();
		if (oHpc == null)	
		{
			System.out.println("ERROR: unable to init OverHpc");
			return;
		}
		int numberOfCpus = oHpc.getNumberOfCpus();
		if (numberOfCpus < 2)
		{
			System.out.println("ERROR: this demo requires at least two CPUs");
			return;	
		}
		System.out.println("   Number of CPUs:    " + numberOfCpus);

		// Init IPMI subsystem:
		OverIpmi oIpmi = OverIpmi.getInstance();
		if (oIpmi == null)
		{
			System.out.println("ERROR: unable to init OverIpmi");
			return;
		}	
		
		int numberOfSensors = oIpmi.getNumberOfSensors();		
		System.out.println("   Number of sensors: " + numberOfSensors);			
		for (int c=0; c<numberOfSensors; c++)
			System.out.println(c + ", " + oIpmi.getSensorName(c));
		double restValue = oIpmi.getSensorValue(systemLevel);
		System.out.println("   Idle level:        " + restValue + " watt\n");
		
		
		
		/////////////////////// 
		// Starting first test:
		System.out.println("*** First pass: using " + numberOfCpus + " CPUs ***");
		INTERLEAVED = true;
		barrier.set(0);		
		for (int c=0; c<numberOfCpus; c++)
		{
			FibonacciThread x = new FibonacciThread();
		}
		
		// Wait for termination:
		while (barrier.get() < numberOfCpus)
		{
			System.out.println("   " + oIpmi.getSensorName(systemLevel) + " = " + oIpmi.getSensorValue(systemLevel) + " watt");
			try 
			{
				Thread.sleep(sleepTime);
			} 
			catch (InterruptedException e) 
			{}			
		}
		
		
		////////////////
		// Cooling down:
		System.out.print("First test completed.\nCooling down to " + restValue + " watt...");
		while (oIpmi.getSensorValue(systemLevel) > restValue)
		{
			System.out.print(".");
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e1) {}
		}
		System.out.println(" Done");		
		
		
		/////////////////////// 
		// Starting second test:		
		System.out.println("*** Second test: using " + numberOfCpus + " cores on a single CPU ***");
		INTERLEAVED = false;
		barrier.set(0);
		for (int c=0; c<numberOfCpus; c++)
		{
			FibonacciThread x = new FibonacciThread();
		}
		
		// Wait for termination:
		while (barrier.get() < numberOfCpus)
		{
			System.out.println("   " + oIpmi.getSensorName(systemLevel) + " = " + oIpmi.getSensorValue(systemLevel) + " watt");
			try 
			{
				Thread.sleep(sleepTime);
			} 
			catch (InterruptedException e) 
			{}			
		}		

		// Done:
		System.out.println("Finished.");
	}
}
