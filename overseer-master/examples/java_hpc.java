import ch.usi.overseer.OverHpc;

public class java_hpc 
{
	public static final int RUNS = 5; 	
	public static final int BIG_ARRAY_SIZE = 10000000;
      
	public static void main(String[] args) throws InterruptedException 
	{
        	// Credits:
        	System.out.println("Overseer Java HPC test, A. Peternier (C) USI 2011\n");

		// Access HPCs:
		OverHpc oHpc = OverHpc.getInstance();
		if (oHpc == null)
		{
			System.out.println("ERROR: unable to init OverHpc");
			return;
		}

		// Check args:		
		if (args.length == 0)
		{
			// Show available events:
			System.out.println("Number of events: " + oHpc.getNumberOfAvailableEvents());
			System.out.println("Events: " + oHpc.getAvailableEventsString());
			System.out.println("\nUsage: test_java_hpc.sh [HPC_NAME]\n");		
			return;			
		}

		// Acquire current process PID:
		int pid = oHpc.getThreadId();
		System.out.println("Main thread PID: " + pid);		
		
		// Init event from args:
		if (oHpc.initEvents(args[0]) == false)
		{
			System.out.println("ERROR: invalid event");
			return;
		}
		oHpc.bindEventsToThread(pid);
		
		// Reading misses for a while:
		System.out.println("\n[Doing " + RUNS + " runs]");
		int bigArray[] = new int[BIG_ARRAY_SIZE];
		long previousValue = oHpc.getEventFromThread(pid, 0);
		for (int c=1; c<=RUNS; c++)
		{
			// Process big array:
			for (int d=0; d<BIG_ARRAY_SIZE; d++)
				bigArray[d] = bigArray[d] + 1;

			// Get and display current value:
			long value = oHpc.getEventFromThread(pid, 0);
			long delta = value - previousValue;
			previousValue = value;
			System.out.println("Run: " + c + ", " + args[0] + ": " + delta + " (tot: " + value + ")");
		}		
		System.out.println("[Done]");
	}
}

