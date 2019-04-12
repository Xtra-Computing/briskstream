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

import java.util.ArrayList;

import java.util.HashMap;
import edu.umass.cs.sase.stream.Event;

/**
 * This class is used to buffer matches
 * @author haopeng
 * 
 */
public class MatchController {
	/**
	 * The matches
	 */
	ArrayList<Match> myMatches;
	/**
	 * The number of matches
	 */
	int myMatchesSize;
	
/**
 * The default constructor
 */
	public MatchController(){
		myMatches = new ArrayList<Match>();
		
		
	}
/**
 * Adds a match to the match buffer.
 * @param m the match to be added
 */
	public void addMatch(Match m){
		this.myMatches.add(m);
	}
/**
 * prints the matches in console
 */
	public void printMatches(){
		for(int i = 0; i < this.myMatches.size(); i ++){
			System.out.println(this.myMatches.get(i));
		}
	}

	/**
	 * @return the myMatches
	 */
	public ArrayList<Match> getMyMatches() {
		return myMatches;
	}
	/**
	 * @param myMatches the myMatches to set
	 */
	public void setMyMatches(ArrayList<Match> myMatches) {
		this.myMatches = myMatches;
	}
	/**
	 * @return the myMatchesSize
	 */
	public int getMyMatchesSize() {
		return myMatchesSize;
	}
	/**
	 * @param myMatchesSize the myMatchesSize to set
	 */
	public void setMyMatchesSize(int myMatchesSize) {
		this.myMatchesSize = myMatchesSize;
	}


}
	
	

