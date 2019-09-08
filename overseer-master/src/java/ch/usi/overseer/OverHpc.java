package ch.usi.overseer;



/**
 * JNI wrapper for the native HPC interface.
 * Installation:
 * 
 *    1. make sure you're running on a 2.6.31+ kernel
 *    2. make sure you've perfmon4 installed (usually in usr/local/lib/libpfm*.*)
 *    3. make sure you've hpcOverseer installed (usually in usr/local/lib/libhpcOverseer*.*)
 *    4. make sure the LIBRARY_PATH is pointing to the correct location containing the wrapper lib (hpcOverseerWrapper.so)  
 * 
 * 
 * History:
 * 
 * 0.8a		- using libhwloc
 * 			- added detection of numas and pus
 * 
 * 0.7a     - initEvents can be called over and over
 *          - added methods to retrieve current events tracked by a thread/core
 * 
 * 0.6a     - added methods to know available supported events
 * 
 * 0.5a     - cpuinfo parsing moved to native code
 *          - fixed non sequential physical id problem (like on SC)
 *          
 * 0.4a		- added RDTSC logging stuffs
 * 
 * 0.3a		- some corollary new methods to make the API more human-friendly
 * 
 * 0.2a 	- singleton version
 *      	- affinity groups reconstructed from system file analysis 
 * 
 * 0.1a 	- first draft
 * 
 * @author Achille Peternier
 * @version 0.8a
 */
public class OverHpc
{
	////////////
	// CONSTS //
	////////////
	
	// Native library location:
	static public final String LIBRARY_PATH = "/usr/local/lib/libhpcOverseerWrapper.so";	
	
	// Version:	
	static public final int VERSION = 8;	///< Divide by 10 for the right number

	// CPU masks:
	static public final long CORE0_MASK 	= 1;
	static public final long CORE1_MASK 	= 2;
	static public final long CORE2_MASK 	= 4;
	static public final long CORE3_MASK 	= 8;
	static public final long CORE4_MASK 	= 16;
	static public final long CORE5_MASK 	= 32;
	static public final long CORE6_MASK 	= 64;
	static public final long CORE7_MASK 	= 128;
	static public final long CORE8_MASK 	= 256;
	static public final long CORE9_MASK 	= 512;
	static public final long CORE10_MASK 	= 1024;
	static public final long CORE11_MASK 	= 2048;
	static public final long CORE12_MASK 	= 4096;
	static public final long CORE13_MASK 	= 8192;
	static public final long CORE14_MASK 	= 16384;
	static public final long CORE15_MASK 	= 32768;		
	static public final long CORE_ALL 		= 65535;
	
	// Again, but in a array:
	static public final long CORE_MASK[] = { CORE0_MASK,
											 CORE1_MASK,
											 CORE2_MASK,
											 CORE3_MASK,
											 CORE4_MASK,
											 CORE5_MASK,
											 CORE6_MASK,
											 CORE7_MASK,
											 CORE8_MASK,
											 CORE9_MASK,
											 CORE10_MASK,
											 CORE11_MASK,
											 CORE12_MASK,
											 CORE13_MASK,
											 CORE14_MASK,
											 CORE15_MASK};
	
	// Simple parameters mnemonic:
	static public final int COUNTER1 = 0;
	static public final int COUNTER2 = 1;
	static public final int COUNTER3 = 2;
	static public final int COUNTER4 = 3;
	static public final int COUNTER5 = 4;
	static public final int COUNTER6 = 5;
	static public final int COUNTER7 = 6;
	static public final int COUNTER8 = 7;
		
	
	
	////////////
	// STATIC //
	////////////	
	
	// Manage native stuff:
	static
	{
		// Load native library:
		System.load(LIBRARY_PATH);	
	}
	
	// Singleton:	
	static private OverHpc singleton = null;
	static private boolean bypassShutdown = false;
		
	
	
	////////////
	// STATIC //
	////////////
	
	static 
	{
		Runtime.getRuntime().addShutdownHook(new Thread()
		{
			public void run()
			{
				OverHpc.shutdown();
			}
		});
	}		
	
	
	
	///////////////
	// SINGLETON //
	///////////////	
	
	/**
	 * Prevent explicit constructors.
	 */
	protected OverHpc()
	{}	
	
	/**
	 * Singleton instance manager.
	 * @return class reference, null if error
	 */
	public static synchronized OverHpc getInstance()
	{
		// Create the object at first occurrence:
		if (singleton == null)
		{
			singleton = new OverHpc();
			if (singleton.init() == false)
			{
				singleton = null;
				return null;
			}			
		}
		
		// Get it:
		return singleton;
	}
	
	/**
	 * Explicit safe shutdown, invoked automatically if the class is used once at least.
	 * You don't need to call it explicitly, since it is automatically invoked through a shutdown hook, but maybe
	 * sometimes you want to close it before the end of the application.
	 * @return true of false if shutdown has really happened
	 */
	public static synchronized boolean shutdown()
	{
		// Singleton used?
		if (singleton != null)
		{
			singleton.terminate();
			if (!bypassShutdown)
				singleton.reset();
			singleton = null;			
		}
		
		// Nothing to do:
		return false;
	}
	
	/**
	 * Avoid to call .reset at shutdown.
	 * @param flag
	 */
	public static void setBypassShutdown(boolean flag)
	{ bypassShutdown = flag; }		
	
	
	
	////////////////////
	// NATIVE METHODS //
	////////////////////
	
	/**
	 * Return HPC native code wrapper library version (divide by 10).
	 * @return current native code version (divide result by 10)
	 */
	public native int getVersion(); 
	
	/**
	 * Initialize the HPC lib.
	 * @return true or false on success/failure
	 */
	private native boolean init();	
	
	/**
	 * Release resources allocated by the HPC lib.
	 * @return true or false on success/failure	 
	 */
	public native boolean terminate();
	
	/**
	 * Return the amount of physical CPU available 
	 * @return number of CPUs
	 */
	public native int getNumberOfCpus();
	
	/**
	 * Return the amount of physical core per cpu available 
	 * @return number of cores per cpu
	 */
	public native int getNumberOfCoresPerCpu();
	
	/**
	 * Return the amount of numa nodes available 
	 * @return number of numa nodes
	 */
	public native int getNumberOfNumas();
	
	/**
	 * Return the amount of cpus allocated per numa node 
	 * @return number of cpus allocated per numa node
	 */
	public native int getNumberOfCpusPerNuma();
	
	/**
	 * Return the amount of physical processing units 
	 * @return number of PUs
	 */
	public native int getNumberOfPus();
	
	/**
	 * Return the cpuid given a cpu and core id
	 * @param cpu cpu id
	 * @param core core number
	 * @return number of cores per CPU
	 */
	public native int getCpuCoreMapping(int cpu, int core);	
	
	/**
	 * Initialize a list of custom events.
	 * @param string comma-separated list of events to track
	 * @return true or false on success/failure
	 */
	public native boolean initEvents(String string);
	
	/**
	 * Get the string of one of the currently initialized events.
	 * @param event event number
	 * @return string with the name and parameters of the event
	 */
	public native String getInitializedEventString(int event);
	
	/**
	 * Returns a list with all the supported events space separated.	 
	 * @return string with the name of all available events
	 */
	public native String getAvailableEventsString();
	
	/**
	 * Bind the previously loaded list of events to a specific core.
	 * @param coreId core id
	 * @return true or false on success/failure
	 */
	public native boolean bindEventsToCore(int coreId);
	
	/**
	 * Bind the previously loaded list of events to a specific thread. 
	 * @param threadId thread PID
	 * @return true or false on success/failure
	 */
	public native boolean bindEventsToThread(int threadId);
	
	/**
	 * Short version of previous method, binding events to the current thread.
	 * @return true or false on success/failure
	 */
	public boolean bindEventsToThread()
	{ return bindEventsToThread(getThreadId()); }
	
	/**
	 * Start recording events.	  
	 * @return true or false on success/failure
	 */
	public native boolean start();
	
	/**
	 * Suspend event recording.
	 * @return true or false on success/failure
	 */
	public native boolean stop();
	
	/**
	 * Get the value returned by a specific event on a specific core.
	 * @param coreId core id
	 * @param event event number
	 * @return the value returned by the counter
	 */
	public native long getEventFromCore(int coreId, int event);
	
	/**
	 * Get the amount of HPCs currently tracked by a core.
	 * @param coreId core id	 
	 * @return the number of HPCs tracked
	 */
	public native long getNumberOfEventsFromCore(int coreId);
	
	/**
	 * Get the name of a HPC event tracked by a core.
	 * @param coreId core id	 
	 * @param event event number
	 * @return the name of the HPC being tracked
	 */
	public native String getEventNameFromCore(int coreId, int event);
	
	/**
	 * Get the value returned by a specific event on a specific thread.
	 * @param threadId thread id
	 * @param event event number (its position in the list passed to initEvents)
	 * @return the value returned by the counter
	 */
	public native long getEventFromThread(int threadId, int event);
	
	/**
	 * Get the amount of HPCs currently tracked by a thread.
	 * @param threadId thread id	 
	 * @return the number of HPCs tracked
	 */
	public native long getNumberOfEventsFromThread(int threadId);
	
	/**
	 * Get the name of a HPC event tracked by a thread.
	 * @param threadId thread id	 
	 * @param event event number
	 * @return the name of the HPC being tracked
	 */
	public native String getEventNameFromThread(int threadId, int event);
	
	/**
	 * Return the amount of events actually ready for binding.
	 * @return number of events ready for binding
	 */
	public native int getNumberOfInitializedEvents();	
	
	/**
	 * Return the amount of events available on this hardware.
	 * @return number of events ready for initialization
	 */
	public native int getNumberOfAvailableEvents();
	
	/**
	 * Return the max PID for threads.
	 * @return MAX_PID
	 */
	public native int getNumberOfThreads();	
	
	/**
	 * Return the PID of the thread from within this method is invoked.
	 * @return PID
	 */
	public native int getThreadId();	
	
	/**
	 * Assign a custom affinity mask to a specific thread.
	 * @param threadId thread id
	 * @param mask bit-mask specifying on which cores this thread shall run
	 * @return true or false on success/failure
	 */
	public native boolean setThreadAffinity(int threadId, long mask);
	
	/**
	 * Assign a custom affinity mask to a specific thread.
	 * @param threadId thread id
	 * @param mask array specifying 0 or 1 for each core
	 * @return true or false on success/failure
	 */
	public native boolean setThreadAffinityMask(int threadId, int mask[]);
	
	/**
	 * Short version of previous method with implicit parameters.
	 * @param mask bit-mask specifying on which cores this thread shall run
	 * @return true of false on success/failure
	 */
	public boolean setThreadAffinity(long mask)
	{ return setThreadAffinity(getThreadId(), mask); }
	
	/**
	 * Get the current affinity mask used by a specific thread.
	 * @param threadId thread id
	 * @return bit-mask specifying on which cores this thread is allowed to run
	 */
	public native long getThreadAffinity(int threadId);
	
	/**
	 * Short version of previous method with implicit parameters.
	 * @return bit-mask specifying on which cores this thread is allowed to run	 * 
	 */
	public long getThreadAffinity()
	{ return getThreadAffinity(getThreadId()); }	
	
	/**
	 * Reset RDTSC logging.
	 */
	public synchronized native void reset();
	
	/**
	 * Add a new entry to the RDTSC log.
	 */
	public synchronized native void setEntry(int paramA, int paramB, int paramC);
	
	/**
	 * Dump RDTSC data to file.
	 * @param filename file name
	 * @return true on success, false otherwise
	 */
	public synchronized native boolean logToFile(String filename);	
}
