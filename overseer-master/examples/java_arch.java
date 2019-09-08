import ch.usi.overseer.OverHpc;

public class java_arch 
{
	public static void main(String[] args) 
	{
        	// Credits:
        	System.out.println("Overseer Java HW Architecture test, A. Peternier (C) USI 2011\n");

		// Access HPCs:
		OverHpc oHpc = OverHpc.getInstance();
		if (oHpc == null)
		{
			System.out.println("ERROR: unable to init OverHpc");
			return;
		}
		
		// Typical Java information:
		System.out.println("JAVA");
		System.out.println("This machine has " + Runtime.getRuntime().availableProcessors() + " core(s).\n");		

		// Hardware survey:
		System.out.println("OVERSEER");
		System.out.println("This machine has " + oHpc.getNumberOfCpus() + 
				   " CPU(s) with " + oHpc.getNumberOfCoresPerCpu() + " core(s) each, for a total of " + 
				   oHpc.getNumberOfPus() + " PU(s).");
		if (oHpc.getNumberOfNumas() > 0)
		{
			System.out.println("It runs on a NUMA architecture with " + oHpc.getNumberOfNumas() + 
					   " node(s) and " + oHpc.getNumberOfCpusPerNuma() + " CPU(s) per node."); 
		}
		else
			System.out.println("It runs on a standard memory hardware.");
		
		// CPU/core ID mapping:
		System.out.println("HW resources are mapped in the following way:");
		for (int cpu = 0; cpu<oHpc.getNumberOfCpus(); cpu++)
			for (int core = 0; core<oHpc.getNumberOfCoresPerCpu(); core++)
				System.out.println("   CPU " + cpu + " core " + core + " has ID " + oHpc.getCpuCoreMapping(cpu, core));				
	}
}

