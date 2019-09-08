import ch.usi.overseer.OverAgent;
import ch.usi.overseer.OverHpc;

/**
 * Overseer.OverAgent sample application.
 * This example shows a basic usage of the OverAgent java agent to keep track of all the threads created
 * by the JVM. Threads include garbage collectors, finalizers, etc. In order to use OverAgent, make sure to 
 * append this option to your command-line:
 * 
 * -agentpath:/usr/local/lib/liboverAgent.so 
 *  
 * @author Achille Peternier (C) 2011 USI 
 */
public class java_agent 
{	
	static public void main(String argv[])
	{
		// Credits:		
        	System.out.println("Overseer Java Agent test, A. Peternier (C) USI 2011\n");					
		
		// Check that -agentpath is working:
		if (OverAgent.isRunning())
			System.out.println("   OverAgent is running");
		else
		{
			System.out.println("   OverAgent is not running (check your JVM settings)");
			return;
		}
		
		// Get some info:
		System.out.println("   Threads running: " + OverAgent.getNumberOfThreads());
		System.out.println();		
		
		OverAgent.initEventCallback(new OverAgent.Callback()
		{
			// Callback invoked at thread creation:
			public int onThreadCreation(int pid, String name) 
			{
				System.out.println("[new] " + name + " (" + pid + ")");
				return 0;
			}

			// Callback invoked at thread termination:
			public int onThreadTermination(int pid, String name) 
			{
				System.out.println("[delete] " + name + " (" + pid + ")");
				return 0;
		}});		
		
		OverHpc oHpc = OverHpc.getInstance();
		int pid = oHpc.getThreadId();
		OverAgent.updateStats();		
		
		// Waste some time:
		double r = 0.0;
		for (int d=0; d<10; d++)
		{
			if (true)
			{
				for (long c=0; c<100000000; c++)
				{
					r += r * Math.sqrt(r) * Math.pow(r, 40.0);
				}
			}
			else			
			{
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {					
					e.printStackTrace();
				}
			}
			OverAgent.updateStats();
			System.out.println("   Thread " + pid + " abs: " + OverAgent.getThreadCpuUsage(pid) + "%, rel: " + OverAgent.getThreadCpuUsageRelative(pid) + "%");
		}		
		
		// Done:		 
		System.out.println("Application terminated");		
	}
}
