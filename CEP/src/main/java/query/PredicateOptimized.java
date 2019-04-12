
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

import java.util.ArrayList;
import java.util.StringTokenizer;


import engine.EventBuffer;
import engine.Run;
import net.sourceforge.jeval.*;
import stream.Event;

/**
 * This class represents a predicate of an edge.
 *  Assumption: no parentheses
 * @author haopeng
 *
 */
public class PredicateOptimized {
	/**
	 * String which describes the predicate, e.g.: price < 100
	 */
	String predicateDescription;
	/**
	 * After parsing, we store the predicate here
	 */
	String formatedPredicate;
	/**
	 * The logical operator in this predicate, one of {>, < , >=, <=, !=, =}
	 */
	String logicalOperator;
	
	/**
	 * The operands on the left side of the logical operator
	 */
	ArrayList<Operand> leftOperands;
	/**
	 * The operands on the right side of the logical operator
	 */
	ArrayList<Operand> rightOperands;
	/**
	 * The aggregation operand in this predicate. We limit that one predicate may contain 0 or 1 aggregation operand
	 */
	Operand aggregationOperand;
	/**
	 * Store the related state for the aggregation operand
	 */
	String relatedState;
	/**
	 * The name of the aggregation
	 */
	String operation;
	/**
	 * The attribute name of the aggregation
	 */
	String attributeName;
	/**
	 * The evaluator
	 * @see net.sourceforge.jeval.Evaluator
	 */
	Evaluator evl;
	/**
	 * Flag denoting whether the state contains parameterized predicates or aggregates.
	 */
	boolean isSingleState;
	/**
	 * Temporary variable to store an operand
	 */
	Operand tempOperand;
	/**
	 * Temporary variable to store an attribute name
	 */
	String tempAttributeName;
	/**
	 * Operands that contain variables
	 */
	ArrayList<Operand> varOperands;
	/**
	 * Default constructor
	 * @param pre the description of a predicate
	 */
	public PredicateOptimized(String pre){
		this.evl = new Evaluator();
		
		this.predicateDescription = pre.trim();
		this.leftOperands = new ArrayList<Operand>();
		this.rightOperands = new ArrayList<Operand>();
		
		this.parsePredicate();
		this.formatPredicate();
		this.linkAggregationOperand();		
		this.checkSingle();
		
	}
	/**
	 * Deals with the aggregation operand
	 */
	public void linkAggregationOperand(){
		for(int i = 0; i < this.varOperands.size(); i ++){
			tempOperand = this.varOperands.get(i);
			if(tempOperand.hasAggregation){
				this.aggregationOperand = tempOperand;
				this.relatedState = tempOperand.getRelatedState();
				this.operation = tempOperand.getAggregation();
				this.attributeName = tempOperand.getAttribute();
				
			}
		}
	}
	/**
	 * Checks if the predicate needs other events
	 */
	public void checkSingle(){
		
		for(int i = 0; i < this.varOperands.size(); i ++){
			tempOperand = this.varOperands.get(i);
			if(!tempOperand.isSingle()){
				this.isSingleState = false;
				return;
			}
		}
		this.isSingleState = true;
	}
	/**
	 * Format the description for evaluation
	 */
	public void formatPredicate(){
		this.varOperands = new ArrayList<Operand>();
		StringBuilder sb = new StringBuilder();
		Operand temp;
		for(int i = 0; i < this.leftOperands.size(); i ++){
			temp = this.leftOperands.get(i);
			if(!temp.operandType.equalsIgnoreCase("nonVar")){
				this.varOperands.add(temp);
			}
			sb.append(temp.formatedRepresentation);
		}
		sb.append(this.logicalOperator);
		for(int i = 0; i < this.rightOperands.size(); i ++){
			temp = this.rightOperands.get(i);
			if(!temp.operandType.equalsIgnoreCase("nonVar")){
				this.varOperands.add(temp);
			}
			sb.append(temp.formatedRepresentation);
		}
		
		this.formatedPredicate = sb.toString();
		

		
		
	}
	/**
	 * Parses the predicate
	 */
	public void parsePredicate(){
		if(this.predicateDescription.contains(">=")){
			this.logicalOperator = ">=";
			this.parseLeftRight(">=");
		}else if(this.predicateDescription.contains("<=")){
			this.logicalOperator = "<=";
			this.parseLeftRight("<=");
		}else if(this.predicateDescription.contains("!=")){
			this.logicalOperator = "!=";
			this.parseLeftRight("!=");
		}else if(this.predicateDescription.contains("==")){
			this.logicalOperator = "==";
			this.parseLeftRight("==");
		}else if(this.predicateDescription.contains("=")){
			this.logicalOperator = "==";
			this.parseLeftRight("=");
		}else if(this.predicateDescription.contains(">")){
			this.logicalOperator = ">";
			this.parseLeftRight(">");
		}else if(this.predicateDescription.contains("<")){
			this.logicalOperator = "<";
			this.parseLeftRight("<");
		}else{
		
			System.exit(0);
		}
	}
	/**
	 * Parses left or right part of the predicate
	 * @param logicOperation the logic operator of this predicate
	 */
	public void parseLeftRight(String logicOperation){
		

		StringTokenizer st = new StringTokenizer(this.predicateDescription, logicOperation);
		String left = st.nextToken();
		String right = st.nextToken();
		this.parseLeftOperand(left);
		this.parseRightOperand(right);
		
	}
	/**
	 * Parses the operands on the left side of the logical operator
	 * @param expression
	 */
	public void parseLeftOperand(String expression){
		int length = expression.length();
		ArrayList<Integer> operatorPosition = new ArrayList<Integer>();
		char temp;
		for(int i = 0; i < length; i ++){
			temp= expression.charAt(i);
			if(temp == '+' || temp == '-' || temp == '*' || temp == '/' || temp == '%'){
				operatorPosition.add(i);
			}
		}
		if(operatorPosition.size() > 0){
		int start = 0;
		int end = -1;
		String subStr;
		String operator;
		for(int i = 0; i < operatorPosition.size(); i ++){
			start = end +1;
			end = operatorPosition.get(i);
			subStr = expression.substring(start, end);
			this.leftOperands.add(new Operand(subStr));
			operator = expression.substring(end, end + 1);
			this.leftOperands.add(new Operand(operator));
		}
		start = end + 1;
		end = expression.length();
		subStr = expression.substring(start, end);
		this.leftOperands.add(new Operand(subStr));
		}else {
			this.leftOperands.add(new Operand(expression));
		}

	}
	/**
	 * Parses the operands on the right side of the operator
	 * @param expression
	 */
	public void parseRightOperand(String expression){
		int length = expression.length();
		ArrayList<Integer> operatorPosition = new ArrayList<Integer>();
		char temp;
		for(int i = 0; i < length; i ++){
			temp= expression.charAt(i);
			if(temp == '+' || temp == '-' || temp == '*' || temp == '/' || temp == '%'){
				operatorPosition.add(i);
			}
		}
		if(operatorPosition.size() > 0){
		int start = 0;
		int end = -1;
		String subStr;
		String operator;
		for(int i = 0; i < operatorPosition.size(); i ++){
			start = end +1;
			end = operatorPosition.get(i);
			subStr = expression.substring(start, end);
			this.rightOperands.add(new Operand(subStr));
			operator = expression.substring(end, end + 1);
			this.rightOperands.add(new Operand(operator));
		}
		start = end + 1;
		end = expression.length();
		subStr = expression.substring(start, end);
		this.rightOperands.add(new Operand(subStr));
		}else {
			this.rightOperands.add(new Operand(expression));
		}

	}


	/**
	 * Evaluates an event against this predicate
	 * @param currentEvent the current event
	 * @param previousEvent previous event in the same run
	 * @return the evaluation result
	 * @throws EvaluationException
	 */
	public boolean evaluate(Event currentEvent, Event previousEvent) throws EvaluationException{

		
		for(int i = 0; i < this.varOperands.size(); i ++){
			tempOperand = this.varOperands.get(i);
			tempAttributeName = tempOperand.getAttribute();
			if(tempOperand.hasRelatedState && tempOperand.relatedState.equalsIgnoreCase("$previous")){
				if(tempOperand.relatedState.equalsIgnoreCase("$previous")){
				
				evl.putVariable(tempOperand.getOriginalRepresentation(), ""+previousEvent.getAttributeByName(tempAttributeName));
				}
			}else {
				evl.putVariable(tempOperand.getOriginalRepresentation(), ""+currentEvent.getAttributeByName(tempAttributeName));
			}
		}
		

		if("1.0".equalsIgnoreCase( evl.evaluate(this.formatedPredicate)))
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
		for(int i = 0; i < this.varOperands.size(); i ++){
			tempOperand = this.varOperands.get(i);
			tempAttributeName = tempOperand.getAttribute();
			if(!tempOperand.hasRelatedState){
				//only need current event
				evl.putVariable(tempOperand.getOriginalRepresentation(), ""+currentEvent.getAttributeByName(tempAttributeName));
			}else if(!tempOperand.hasAggregation){
				// need related event
				if(tempOperand.getRelatedState().equalsIgnoreCase("$previous")){
					int eventId = r.getPreviousEventId();
					int value = b.getEvent(eventId).getAttributeByName(tempAttributeName);
					
					evl.putVariable(tempOperand.getOriginalRepresentation(), ""+b.getEvent(eventId).getAttributeByName(tempAttributeName));
				}
			}else{
				// need aggregation value
				int stateNumber;
				stateNumber = Integer.parseInt(tempOperand.getRelatedState());
				if(stateNumber - 1 == r.getCurrentState()){
					return true;
				}
				evl.putVariable(tempOperand.getOriginalRepresentation(), ""+r.getNeededValueVector(stateNumber - 1, tempAttributeName, tempOperand.getAggregation()));
			}
			}
		
		
		

		if("1.0".equalsIgnoreCase( evl.evaluate(this.formatedPredicate)))
		   return true;
		else
			return false;
	}
	/**
	 * @return the predicateDescription
	 */
	public String getPredicateDescription() {
		return predicateDescription;
	}
	/**
	 * @param predicateDescription the predicateDescription to set
	 */
	public void setPredicateDescription(String predicateDescription) {
		this.predicateDescription = predicateDescription;
	}
	/**
	 * @return the formatedPredicate
	 */
	public String getFormatedPredicate() {
		return formatedPredicate;
	}
	/**
	 * @param formatedPredicate the formatedPredicate to set
	 */
	public void setFormatedPredicate(String formatedPredicate) {
		this.formatedPredicate = formatedPredicate;
	}
	/**
	 * @return the logicalOperator
	 */
	public String getLogicalOperator() {
		return logicalOperator;
	}
	/**
	 * @param logicalOperator the logicalOperator to set
	 */
	public void setLogicalOperator(String logicalOperator) {
		this.logicalOperator = logicalOperator;
	}

	/**
	 * @return the leftOperands
	 */
	public ArrayList<Operand> getLeftOperands() {
		return leftOperands;
	}
	/**
	 * @param leftOperands the leftOperands to set
	 */
	public void setLeftOperands(ArrayList<Operand> leftOperands) {
		this.leftOperands = leftOperands;
	}
	/**
	 * @return the rightOperands
	 */
	public ArrayList<Operand> getRightOperands() {
		return rightOperands;
	}
	/**
	 * @param rightOperands the rightOperands to set
	 */
	public void setRightOperands(ArrayList<Operand> rightOperands) {
		this.rightOperands = rightOperands;
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
	 * @return the varOperands
	 */
	public ArrayList<Operand> getVarOperands() {
		return varOperands;
	}
	/**
	 * @param varOperands the varOperands to set
	 */
	public void setVarOperands(ArrayList<Operand> varOperands) {
		this.varOperands = varOperands;
	}
	/**
	 * @return the aggregationOperand
	 */
	public Operand getAggregationOperand() {
		return aggregationOperand;
	}
	/**
	 * @param aggregationOperand the aggregationOperand to set
	 */
	public void setAggregationOperand(Operand aggregationOperand) {
		this.aggregationOperand = aggregationOperand;
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
	 * @return the tempOperand
	 */
	public Operand getTempOperand() {
		return tempOperand;
	}
	/**
	 * @param tempOperand the tempOperand to set
	 */
	public void setTempOperand(Operand tempOperand) {
		this.tempOperand = tempOperand;
	}
	/**
	 * @return the tempAttributeName
	 */
	public String getTempAttributeName() {
		return tempAttributeName;
	}
	/**
	 * @param tempAttributeName the tempAttributeName to set
	 */
	public void setTempAttributeName(String tempAttributeName) {
		this.tempAttributeName = tempAttributeName;
	}
	
}
