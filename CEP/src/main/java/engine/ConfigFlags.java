
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
 * This ConfigFlags class sets the parameters for the Engine.<br>
 * @author haopeng
 * 
 * 
 */
public class ConfigFlags {
	

	/**
	 * The engine will output the results in console if we set it as true.
	 */
	
	public static boolean printResults = true;
	
	/**
	 * The engine will generate random values for stream if we set it as true.
	 */
	
	public static boolean useRandomStream = false;
	/**
	 * This field denotes the selection strategy which is being used.
	 */
	
	public static String selectionStrategy = "skip-till-any-match" ;
	
	/**
	 * This field describes the length of a query
	 */
	
	public static int sequenceLength;
	
	/**
	 * This field describes the time window in the query
	 */
	public static int timeWindow;
	
	/**
	 * This filed is used to denote whether or not using the sharing technique.
	 */
	
	public static boolean useSharing = false;
	
	/**
	 * This filed is used to denote whether the query has a partition attribute.
	 */
	
	public static boolean hasPartitionAttribute = false;
	
	/**
	 * This filed is used to denote the name of the partition attribute.
	 */
	public static String partitionAttribute;
	
	/**
	 * This field is used denote whether the query has a negation component.
	 */
	public static boolean hasNegation = false;
	
	
	
}
