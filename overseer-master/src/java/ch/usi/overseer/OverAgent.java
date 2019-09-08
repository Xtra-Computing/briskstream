package ch.usi.overseer;



/**
 * JNI interface for the OverAgent Java agent.
 * Installation:
 *
 *    1. make sure you started the JVM with the -agentpath: option
 *    2. make sure that -agentpath: option points to the proper path where liboverAgent.so is located
 *
 *
 * History:
 * 
 *  0.1a 	- first draft
 *  
 * @author Achille Peternier
 * @version 0.1a
 */
public class OverAgent 
{
	////////////
	// CONSTS //
	////////////
	
	// Native library location:
	static public final String LIBRARY_PATH = "/usr/local/lib/liboverAgent.so";
	
	// Version:	
	static public final int VERSION = 2;	///< Divide by 10 for the right number
	
	// Event codes:	
	static public final int THREAD_CREATED 		= 0;	///< A new thread has been created
	static public final int THREAD_TERMINATED 	= 1;	///< A new thread has been created
	
	// Custom callback:
	static private Callback callback = null;

	
	
	////////////////
	// INTERFACES //
	////////////////
	
	public interface Callback
	{
		int onThreadCreation(int pid, String threadName);
		int onThreadTermination(int pid, String threadName);		
	}

	
	
	////////////
	// STATIC //
	////////////	
	
	// Manage native stuff:
	static
	{
		// Load native library:
		System.load(LIBRARY_PATH);		
	}
	
	
	
	////////////////////
	// NATIVE METHODS //
	////////////////////
	
	/**
	 * Returns true if the OverAgent library has been correctly started as JVM agent with the option -agentpath.
	 * @return true if the agent is working
	 */
	public static native boolean isRunning();
	
	/**
	 * Returns OverAgent interface library version (divide by 10).
	 * @return current native code version (divide result by 10)
	 */	
	public static native int getVersion();	
	
	/**
	 * Returns current number of threads running within the JVM.
	 * @return number of threads running in the JVM
	 */
	public static native int getNumberOfThreads();
	
	/**
	 * Returns the cpu usage percent of a thread given its pid.
	 * @return cpu usage or -1.0 if wrong
	 */
	public static native float getThreadCpuUsage(int pid);
	
	/**
	 * Returns the cpu usage percent of a thread given its pid relative to full power.
	 * @return cpu usage or -1.0 if wrong
	 */
	public static native float getThreadCpuUsageRelative(int pid);
	
	/**
	 * Updates the stats for all the registered threads.
	 */
	public static native void updateStats();
	
	/**
	 * Start signal for the native agent to send callbacks to this class.
	 * Call it only once the customized callback has been defined.
	 */
	private static native void init();
	
		
	
	/////////////
	// METHODS //
	/////////////
	
	/**
	 * Callback invoked by native code to communicate events to java-code level.
	 * @param event event code (see enumeration)
	 * @param intData generic data, according to the event kind, of type integer
	 * @param strData generic data, according to the event kind, of type string
	 */
	public static void eventCallback(int event, int intData, String strData)
	{
		// Agent not loaded, bypass:
		if (!isRunning())
			return;
		
		// Default implementation:
		if (callback == null)
		{
			System.out.println("New event received");
			System.out.println("   Event ID: " + event);
			System.out.println("   Int data: " + intData);
			System.out.println("   Str data: " + strData);
		}
		else // Customized one:		
			switch (event)
			{
				case THREAD_CREATED:					
						callback.onThreadCreation(intData, strData);
					break;
					
				case THREAD_TERMINATED:					
						callback.onThreadTermination(intData, strData);
					break;
			}
	}
	
	/**
	 * Sets a custom callback for thread creation/termination notification.
	 * @param callback class implementing the callback interface
	 */
	public static void initEventCallback(Callback callback)
	{		
		// Agent not loaded, bypass:
		if (!isRunning())
			return;
		
		OverAgent.callback = callback;
		
		// Starts sending callbacks:
		init();
	}
}
