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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * This class reads the configuration file for the input stream.
 * @author haopeng
 *
 */

/**
 * Constructor
 */
public class ParseStockStreamConfig {
	public static void parseStockEventConfig(String configFile) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(configFile));
		String line;
		while(!(line = br.readLine()).equalsIgnoreCase("end")){
			parseLine(line);
		}
	}
	
	/**
	 * Parses a line in the configuration file
	 * @param line The input line
	 */
	public static void parseLine(String line){
		StringTokenizer st = new StringTokenizer(line, "=");
		String attribute = st.nextToken();
		String v = st.nextToken();
		int value = Integer.parseInt(v.trim());
		setAttributeRange(attribute, value);
		
	}
	
	/**
	 * Sets the attribute range for an attribute
	 * @param attribute The attribute to be set
	 * @param value The maximu value to be set
	 */
	public static void setAttributeRange(String attribute, int value){
		if(attribute.trim().equalsIgnoreCase("streamSize")){
			StockStreamConfig.streamSize = value;
		}else if(attribute.trim().equalsIgnoreCase("maxPrice")){
			StockStreamConfig.maxPrice = value;
		}else if(attribute.trim().equalsIgnoreCase("numOfSymbol")){
			StockStreamConfig.numOfSymbol = value;
		}else if(attribute.trim().equals("maxVolume")){
			StockStreamConfig.maxVolume = value;
		}else if(attribute.trim().equalsIgnoreCase("randomSeed")){
			StockStreamConfig.randomSeed = value;
		}else if(attribute.trim().equalsIgnoreCase("increaseProbability")){
			StockStreamConfig.increaseProbability = value;
		}
	}
}
