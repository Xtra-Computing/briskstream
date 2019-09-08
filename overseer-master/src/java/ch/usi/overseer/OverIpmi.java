package ch.usi.overseer;

public class OverIpmi 
{
	////////////
	// CONSTS //
	////////////
	
	// Native library location:
	static public final String LIBRARY_PATH = "/usr/local/lib/liboverIpmi.so";
	
	// Version:	
	static public final int VERSION = 1;	///< Divide by 10 for the right number	
	
	// Flag:
	private boolean running = false;
		
	// Singleton:	
	private static OverIpmi singleton = null;
	
	
	
	////////////////////////////////
	// Class OverIpmiShutdownHook //
	////////////////////////////////
	
	/**
	 * Automatic shutdown of native resources.
	 */
	private class OverIpmiShutdownHook extends Thread
	{
		/**
		 * Method invoked at application termination, freeing resources.
		 */
		public synchronized void run() 
		{ 
			if (singleton != null)
			{
				if (singleton.running)
					singleton.free();				
				singleton = null;			
			}			
		}			
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
	 * Initializes native code part.
	 * @return true or false
	 */
	private native boolean init();
	
	/**
	 * Releases native code part.
	 * @return true or false
	 */
	private native boolean free();
	
	/**
	 * Get number of available sensor ids.
	 * @return max number of ids
	 */
	public native int getNumberOfSensors();
	
	/**
	 * Retrieve sensor name, given its id.
	 * @param sensorId sensor id
	 * @return name
	 */
	public native String getSensorName(int sensorId);
	
	/**
	 * Retrieve sensor value, given its id.
	 * @param sensorId sensor id
	 * @return value
	 */
	public native double getSensorValue(int sensorId);
	
	
	
	/////////////
	// METHODS //
	/////////////
	
	/**
	 * Prevent explicit constructors.
	 */
	protected OverIpmi()
	{}
	
	/**
	 * Register shutdown.
	 */
	private void registerShutdown()
	{
		// Assure a clean shutdown of native resources:
		Runtime.getRuntime().addShutdownHook(new OverIpmiShutdownHook());
	}
	
	/**
	 * Singleton instance manager.
	 * @return class reference, null if error
	 */
	public static synchronized OverIpmi getInstance()
	{
		// Create the object at first occurrence:
		if (singleton == null)
		{
			singleton = new OverIpmi();
			if (singleton.init() == false)
			{
				singleton = null;
				return null;
			}
			singleton.registerShutdown();
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
	public static boolean shutdown()
	{
		// Singleton used?
		if (singleton != null)
		{
			singleton.free();			
			singleton = null;			
		}
		
		// Nothing to do:
		return false;
	}
}
