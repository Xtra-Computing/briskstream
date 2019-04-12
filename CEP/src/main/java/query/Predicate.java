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
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import edu.umass.cs.sase.engine.EventBuffer;
import edu.umass.cs.sase.engine.Run;
import edu.umass.cs.sase.stream.Event;

import net.sourceforge.jeval.*;
/**
 * This class represents a predicate of an edge.
 * @author haopeng
 *
 */


public class Predicate {
	/**
	 * String which describes the predicate, e.g.: price < 100
	 */
	String predicate;
	
	/**
	 * The attribute used in the predicate, e.g.: price
	 */
	String attributeName;
	
	/**
	 * The elements in the predicate, used to compose the string for evaluation
	 */
	String[] elements;
	
	/**
	 * The operands in the predicate, used to get attribute value from events
	 */
	String[] operands;
	
	/**
	 * The number of operands
	 */
	int operandCount;
	
	/**
	 * The evaluator
	 * @see net.sourceforge.jeval.Evaluator
	 */
	Evaluator evl;
	
	/**
	 * The aggregation in the predicate, if any
	 */
	String operation;
	
	/**
	 * Flag denoting whether the state contains parameterized predicates or aggregates.
	 */
	boolean isSingleState;
	
	/**
	 * Describes the related state in parameterized predicates or aggregates.
	 */
	String relatedState; 
	
	/**
	 * Constructs a predicate from the input description string.
	 * @param p the input description string
	 */
	public Predicate(String p){// this constructor parses a predicate from a string from nfa file
		if(p.contains("$") && !p.contains("$previous")){//do not use $ as other symbols $ is the symbol for cross state predicate
			this.setSingleState(false);
			this.computeRelatedState(p);
		}else{
			this.setSingleState(true);
			
		}
		StringTokenizer st = new StringTokenizer(p);
		this.evl = new Evaluator();
		int size = st.countTokens(); // use space as the delim
		elements = new String[size];
		operands = new String[size];
		String operandPattern ="[a-zA-Z$].+";
		int countOperand = 0;
		int countElement = 0;
		String temp = "";
		while(st.hasMoreTokens()){
			temp = st.nextToken().trim();// get one element
			if(temp.contains("(")){
				temp = temp.replace('(', '.');
				temp = temp.replace(')', ' ');
			}
			if(temp.matches(operandPattern)){// if it is an element
				
				operands[countOperand] = temp;
				elements[countElement] = "#{"+temp+"}";// would use the wrapper in order to use the format of jeval package.
				countOperand ++;
				countElement ++;
			}else{
				elements[countElement] = temp;// operators such as +/-/*/</> etc
				countElement ++;
			}
				
		}
		
		temp = "";
		for (int i = 0; i < size; i ++){
			temp += elements[i]; // to compose the string for jeval
		}
		predicate = temp;
		this.operandCount = countOperand;
		for(int i = 0; i < countOperand; i ++){
			if(!(operands[i].startsWith("$"))){
			
				this.attributeName = operands[i];// get the attribute name
				break;
			
			}
		}
		
		if(!this.isSingleState){
		if (this.predicate.contains("avg.")){
			this.operation = "avg(";
			
			
		}else if(this.predicate.contains("max.")){
			this.operation = "max(";
		}else if(this.predicate.contains("min.")){
			this.operation = "min(";
		}else {
			this.operation = "set";
		}
		}
		
	}
	
	/**
	 * Computes the related state
	 * @param predicate the description string
	 */
	public void computeRelatedState(String predicate){
		
		int startPosition = predicate.indexOf('$')+1;
		int endPosition = predicate.indexOf('.');
		String sub = predicate.substring(startPosition,endPosition);
		this.setRelatedState(sub);
		
	}
	
	/**
	 * Evaluates an event against this predicate
	 * @param currentEvent the current event
	 * @param previousEvent previous event in the same run
	 * @return the evaluation result
	 * @throws EvaluationException
	 */
	public boolean evaluate(Event currentEvent, Event previousEvent) throws EvaluationException{
		for(int i = 0; i < this.operandCount; i ++){
			if(operands[i].startsWith("previous")){
				evl.putVariable(operands[i], ""+previousEvent.getAttributeByName(attributeName));
				
			}else{
				evl.putVariable(operands[i], ""+currentEvent.getAttributeByName(attributeName));
			}
		}
		if("1.0".equalsIgnoreCase( evl.evaluate(predicate)))
		   return true;
		else
			return false;
	}
	

	
	/**
	 * Evaluates an event against the predicate
	 * @param currentEvent the current event
	 * @param r the run
	 * @return the evaluation result
	 * @throws EvaluationException
	 */
	public boolean evaluate(Event currentEvent, Run r, EventBuffer b) throws EvaluationException{
		for(int i = 0; i < this.operandCount; i ++){
			if(!operands[i].contains("$")){
				evl.putVariable(operands[i], ""+currentEvent.getAttributeByName(attributeName));
			}else if(operands[i].contains("$previous")){
				int eventId = r.getPreviousEventId();
				evl.putVariable(operands[i], ""+ b.getEvent(eventId).getAttributeByName(attributeName));
			}
			else {
				int stateNumber;
				// for aggreagations
					stateNumber = Integer.parseInt(this.relatedState);
					if(stateNumber -1 == r.getCurrentState()){
						return true;// not initialized yet
					}
				
				
				evl.putVariable(operands[i], ""+r.getNeededValueVector(stateNumber-1, attributeName, this.operation));
								
			}
		}
		if("1.0".equalsIgnoreCase( evl.evaluate(predicate)))
		   return true;
		else
			return false;
	}

	/**
	 * Self description
	 */
	public String toString(){
		return predicate;
	}

	/**
	 * @return the isSingleState
	 */
	public boolean isSingleState() {
		return isSingleState;
	}

	/**
	 * @param isSingleState the isSingleState to set
	 */
	public void setSingleState(boolean isSingleState) {
		this.isSingleState = isSingleState;
	}

	/**
	 * @return the predicate
	 */
	public String getPredicate() {
		return predicate;
	}

	/**
	 * @param predicate the predicate to set
	 */
	public void setPredicate(String predicate) {
		this.predicate = predicate;
	}

	/**
	 * @return the attributeName
	 */
	public String getAttributeName() {
		return attributeName;
	}

	/**
	 * @param attributeName the attributeName to set
	 */
	public void setAttributeName(String attributeName) {
		this.attributeName = attributeName;
	}

	/**
	 * @return the elements
	 */
	public String[] getElements() {
		return elements;
	}

	/**
	 * @param elements the elements to set
	 */
	public void setElements(String[] elements) {
		this.elements = elements;
	}

	/**
	 * @return the operands
	 */
	public String[] getOperands() {
		return operands;
	}

	/**
	 * @param operands the operands to set
	 */
	public void setOperands(String[] operands) {
		this.operands = operands;
	}

	/**
	 * @return the operandCount
	 */
	public int getOperandCount() {
		return operandCount;
	}

	/**
	 * @param operandCount the operandCount to set
	 */
	public void setOperandCount(int operandCount) {
		this.operandCount = operandCount;
	}

	/**
	 * @return the evl
	 */
	public Evaluator getEvl() {
		return evl;
	}

	/**
	 * @param evl the evl to set
	 */
	public void setEvl(Evaluator evl) {
		this.evl = evl;
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
	 * @return the relatedState
	 */
	public String getRelatedState() {
		return relatedState;
	}
	/**
	 * @param relatedState the relatedState to set
	 */
	public void setRelatedState(String relatedState) {
		this.relatedState = relatedState;
	}
	
	
	
}
