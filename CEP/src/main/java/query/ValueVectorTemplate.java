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
package query;

/**
 * This class represents a value vector template, which is extracted from a query while the compiling is being done.
 * @author haopeng
 *
 */
public class ValueVectorTemplate {
	
	/**
	 * The number of the state from which this value vector gets its value
	 */
	int state;
	
	/**
	 * The name of the attribute from which this value vector gets its value
	 */
	String attribute;
	
	/**
	 * In what operation the value vector gets its value, including aggregates(max, min, avg), and parameterized predicates(set)
	 */
	String operation;
	int neededByState;
	public ValueVectorTemplate(int s, String a, String o, int n){
		this.setState(s);
		this.setAttribute(a);
		this.setOperation(o);
		this.setNeededByState(n);
	}
	
	/**
	 * Self description
	 */
	public String toString(){
		String s = "";
		s += "The state is:";
		s += this.getState() + "\n";
		s += "The attribute is:";
		s += this.getAttribute() + "\n";
		s += "The operation is:";
		s += this.getOperation() + "\n";
		s += "The value vector is needed by State ";
		s += this.getNeededByState();
		return s;
		
	}
	/**
	 * @return the state
	 */
	public int getState() {
		return state;
	}
	/**
	 * @param state the state to set
	 */
	public void setState(int state) {
		this.state = state;
	}
	/**
	 * @return the attribute
	 */
	public String getAttribute() {
		return attribute;
	}
	/**
	 * @param attribute the attribute to set
	 */
	public void setAttribute(String attribute) {
		this.attribute = attribute;
	}
	/**
	 * @return the operation
	 */
	public String getOperation() {
		return operation;
	}
	/**
	 * @param operation the operation to set
	 */
	public void setOperation(String operation) {
		this.operation = operation;
	}
	/**
	 * @return the neededByState
	 */
	public int getNeededByState() {
		return neededByState;
	}
	/**
	 * @param neededByState the neededByState to set
	 */
	public void setNeededByState(int neededByState) {
		this.neededByState = neededByState;
	}
	
	
	
}
