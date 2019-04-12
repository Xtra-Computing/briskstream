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
 * This class represents the MIN aggregation.
 * @author haopeng
 *
 */
public class ValueVectorElementMin implements ValueVectorElement{
	int stateNumber;
	String attribute;
	int currentValue;
	
	int neededByState;
	/* (non-Javadoc)
	 * @see engine.ValueVectorElement#getValue()
	 */
	public int getValue() {
		// TODO Auto-generated method stub
		return this.currentValue;
	}
	/* (non-Javadoc)
	 * @see engine.ValueVectorElement#updateValue(stream.Event)
	 */
	public void updateValue(Event e) {
		// TODO Auto-generated method stub
		int temp = e.getAttributeByName(attribute);
		if(temp < this.currentValue){
			this.currentValue = temp;
		}
		
	}
	/* (non-Javadoc)
	 * @see engine.ValueVectorElement#getStateNumber()
	 */
	public int getStateNumber() {
		// TODO Auto-generated method stub
		return this.stateNumber;
	}
	/* (non-Javadoc)
	 * @see engine.ValueVectorElement#getType()
	 */
	public String getType() {
		// TODO Auto-generated method stub
		return "min";
	}
	/* (non-Javadoc)
	 * @see engine.ValueVectorElement#getAttribute()
	 */
	public String getAttribute() {
		// TODO Auto-generated method stub
		return this.attribute;
	}
	/* (non-Javadoc)
	 * @see engine.ValueVectorElement#initializeValue(stream.Event)
	 */
	public void initializeValue(Event e) {
		// TODO Auto-generated method stub
		this.currentValue = e.getAttributeByName(attribute);
		
	}
	/* (non-Javadoc)
	 * @see engine.ValueVectorElement#setAttribute(java.lang.String)
	 */
	public void setAttribute(String a) {
		// TODO Auto-generated method stub
		this.attribute = a;
		
	}
	/* (non-Javadoc)
	 * @see engine.ValueVectorElement#getNeededByState()
	 */
	public int getNeededByState() {
		// TODO Auto-generated method stub
		return this.neededByState;
	}
	/**
	 * @param neededByState the neededByState to set
	 */
	public void setNeededByState(int neededByState) {
		this.neededByState = neededByState;
	}
	

}
