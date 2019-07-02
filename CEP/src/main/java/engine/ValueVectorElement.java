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


import stream.Event;

/**
 * This is an interface for the value vector of the computation state.
 * If you add any new class implementing this interface, please also add the reference point in the following places:
 * -engine.Run.initializeValueVector(Event e)
 * @author haopeng
 *
 */
public interface ValueVectorElement {
	/**
	 * Gets the current value
	 * @return the current value
	 */
    int getValue();
	
	/**
	 * Updates the value
	 * @param e the newly selected event
	 */
    void updateValue(Event e);
	
	/**
	 * 
	 * @return the state number of the value
	 */
    int getStateNumber();
	
	/**
	 * 
	 * @return the opearation type, avg, max, min, set
	 */
    String getType();
	
	/**
	 * 
	 * @return the attribute name
	 */
    String getAttribute();
	
	/**
	 * 
	 * @param a the attribute name to set
	 */
    void setAttribute(String a);
	
	/**
	 * initializes the value by an eventg
	 * @param e
	 */
    void initializeValue(Event e);

	int getNeededByState();
	void setNeededByState(int n);
}
