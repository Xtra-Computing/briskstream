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
package stream;

/**
 * Configurates the paramters for generating stock event streams
 * @author haopeng
 *
 */
public class StockStreamConfig {
	/**
	 * 
	 */
	public static int streamSize = 10000;
	
	/**
	 * Denoting the max price value, so the price range is: [1, maxPrice]
	 */
	public static int maxPrice = 100;
	
	/**
	 * Volume range is: [1, maxVolume]
	 */
	public static int maxVolume = 1000;
	
	/**
	 * Symbol range is: [1, numOfSymbol];
	 */
	public static int numOfSymbol = 10;
	
	/**
	 * The random seed.
	 * 
	 */
	
	public static int randomSeed = 11;
	
	/**
	 * The probability that the value would increase
	 */
	
	public static int increaseProbability = 101; //%
	
	/**
	 * Prints the configuration
	 */
	public static void printConfig(){
		System.out.println("stream size:" + streamSize);
		System.out.println("price range: [1, " + maxPrice + "]");
		System.out.println("volume range: [1, " + maxVolume + "]");
		System.out.println("symbol range: [1, " + numOfSymbol + "]");
		System.out.println("random seed:" + randomSeed );
		System.out.println("increase probability: " + increaseProbability + "%");
		
	}
}
