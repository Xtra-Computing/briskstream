/*
 * Copyright (c) 2011, Regents of the University of Massachusetts Amherst 
 * All rights reserved.

 * Redistribution and use in source and binary forms, with or without modification, are permitted 
 * provided that the following conditions are met:

 *   * Redistributions of source code must retain the above copyright notice, this list of conditions 
 * 		and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright notice, this list of conditions 
 * 		and the following disclaimer in the documentation and/or other materials provided with the distribution.
 *   * Neither the name of the University of Massachusetts Amherst nor the names of its contributors 
 * 		may be used to endorse or promote products derived from this software without specific prior written 
 * 		permission.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR 
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES 
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; 
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, 
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF 
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
package engine;


/**
 * This class profiles the numbers of performance
 * @author haopeng
 *
 */
public class Profiling {
	
	/**
	 * The total running time of the engine (nanoseconds)
	 */
	public static long totalRunTime = 0;
	/**
	 * The number of events processed
	 */
	public static long numberOfEvents = 0;
	/**
	 * The number of runs generated
	 */
	public static long numberOfRuns = 0;
	/**
	 * The total run lifetime
	 */
	public static long totalRunLifeTime = 0;
	/**
	 * The number of matches
	 */
	public static long numberOfMatches = 0;
	/**
	 * The number of runs which ends before reach the final state
	 */
	public static long numberOfRunsCutted = 0;
	/**
	 * The number of runs which are deleted because of they violate the time window constraint
	 */
	public static long numberOfRunsOverTimeWindow = 0;
	/**
	 * The cost on match construction
	 */
	public static long timeOfMatchConstruction = 0;
	
	public static long negatedMatches = 0;
	
	/**
	 * resets the profiling numbers
	 */
	public static void resetProfiling(){
		
		 totalRunTime = 0;
		 numberOfEvents = 0;
		 numberOfRuns = 0;
		 totalRunLifeTime = 0;
		 numberOfMatches = 0;
		 numberOfRunsCutted = 0;
		 numberOfRunsOverTimeWindow = 0;
		 timeOfMatchConstruction = 0;
		 numberOfMergedRuns = 0;
		 negatedMatches = 0;
	}
	
	/**
	 * prints the profiling numbers in console
	 */
	public static void printProfiling(){
		
		System.out.println();
		System.out.println("**************Profiling Numbers*****************");
		System.out.println("Total Running Time: " + totalRunTime + " nanoseconds");
		System.out.println("Number Of Events Processed: " + numberOfEvents);
		System.out.println("Number Of Runs Created: " + numberOfRuns);
		System.out.println("Number Of Matches Found: " + numberOfMatches);
		if(ConfigFlags.hasNegation){
			System.out.println("Number of Negated Matches: " + negatedMatches );
		}
	
		
		long throughput1 = 0;
		if(totalRunTime > 0){
			throughput1 = numberOfEvents* 1000000000/totalRunTime ;
			System.out.println("Throughput: " + throughput1 + " events/second");
		}
		
		
		

		
		
	}


	

	
	// sharing numbers
	/**
	 * number of runs merged in the sharing engine
	 */
	public static long numberOfMergedRuns = 0;
	public static void resetSharingProfiling(){
		numberOfMergedRuns = 0;
	}
	/**
	 * outputs the extra profiling numbers for the sharing engine
	 */
	public static void printSharingProfiling(){
		System.out.println("#Merged Runs = " + numberOfMergedRuns);
	}

}
