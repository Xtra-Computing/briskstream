
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
 * This class wraps an operand
 * @author haopeng
 *
 */
public class Operand {
	/**
	 * Describes the type of the operand. 
	 * The type should be one of the following options:
	 * nonVar: for number and operator 
	 * attributeVar: for an attribute name 
	 * relatedEventVar: for a related event's attribute 
	 * aggregationVar: for an aggregation
	 */
	String operandType; //nonVar: for number and operator, attributeVar: for an attribute name, relatedEventVar: for a related event's attribute, aggregationVar: for an aggregation
	/**
	 * Original representation of the operand, e.g. price
	 */
	String originalRepresentation;
	/**
	 * Formatted representation of the operand, preperation for evaluation
	 */
	String formatedRepresentation;
	/**
	 * The attribute in the operand
	 */
	String attribute;
	
	/**
	 * The aggregation in the operand
	 */
	String aggregation;
	/**
	 * The related state of the operand
	 */
	String relatedState;
	/**
	 * Flag denoting whether the operand has a related state
	 */
	boolean hasRelatedState;
	/**
	 * Flag denoting whether the operand has an aggregation
	 */
	boolean hasAggregation;
	/**
	 * Flag denoting whether the operand needs other events
	 */
	boolean isSingle;
	/**
	 * Flag denoting whether the operand is a number
	 */
	boolean isNumber;
	/**
	 * Default constructor
	 * @param oper the description of the operand
	 */
	public Operand(String oper){
		
		oper = oper.trim();
		this.originalRepresentation = oper;
		this.parse(oper);
		this.formatRepresentation();
		this.checkSingle();
		
	}
	/**
	 * Checks whether the operand is single
	 */
	public void checkSingle(){
		if(this.hasAggregation){
			this.isSingle = false;
			return;
		}else if(this.hasRelatedState && !this.relatedState.equalsIgnoreCase("$previous")){
			this.isSingle = false;
			return;
		}
		this.isSingle = true;
	}
	/**
	 * Parses the operand
	 * @param operand the operand description
	 */
	public void parse(String operand){
		String numPattern = "[0-9]+";
		String operatorPattern = "[-'+''*''/''%']";
		if(operand.matches(numPattern) || operand.matches(operatorPattern)){
			
			this.operandType = "nonVar";
			
			
			
		}else{
			
			this.parseNonNum(operand);
		}
	}
	/**
	 * Parses the non number operands
	 * @param operand
	 */
	public void parseNonNum(String operand){
		// 3 cases:
		// -1. attribute name, 
		// -2. other event's attribute name, starting with '$'
		// -3. aggregation: feature: contains'(' and ')'
		if(operand.contains("(") && operand.contains(")")){
			this.operandType = "aggregationVar";
			this.parseAggregation(operand);
			
		}else if(operand.startsWith("$")){
			this.operandType = "relatedEventVar";
			this.parseRelatedEvent(operand);
		}else{
			this.operandType = "attributeVar";
			this.attribute = operand;
			this.hasAggregation = false;
			this.hasRelatedState = false;
		}
		
	}
	/**
	 * Parses the aggregation
	 * @param operand
	 */
	public void parseAggregation(String operand){
		
		this.hasAggregation = true;
		this.hasRelatedState = true;
		int position = operand.indexOf('(');	
		this.aggregation = operand.substring(0, position);	
		this.parseRelatedEventForAggregation(operand.substring(position + 1, operand.length() - 1));
	}
	/**
	 * Parses related event for aggregation
	 * @param operand
	 */
	public void parseRelatedEventForAggregation(String operand){
		
		
		int position = operand.indexOf('.');
		this.relatedState = operand.substring(1, position);
	
		this.attribute = operand.substring(position + 1);
	
	}
	/**
	 * Parses related event
	 * @param operand
	 */
	public void parseRelatedEvent(String operand){
		this.hasRelatedState = true;
	
		int position = operand.indexOf('.');
		this.relatedState = operand.substring(0, position);
		
		if(this.relatedState.equalsIgnoreCase("$previous")){
			this.hasAggregation = false;
			this.relatedState = operand.substring(0, position);
		
		}else {
			this.hasAggregation = true;
			this.aggregation = "set";
			this.relatedState = operand.substring(1, position);
			this.operandType = "aggregationVar";
		
		}
		
		this.attribute = operand.substring(position + 1);
		
	}
	/**
	 * Format the representation of the operand
	 */
	public void formatRepresentation(){
		if(this.operandType.equalsIgnoreCase("nonVar")){
			this.formatedRepresentation = this.originalRepresentation;
		}else {
			this.formatedRepresentation = "#{" + this.originalRepresentation + "}";
		}

	}
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("My operand type is: " + this.operandType + "\n");
		if(this.operandType.equalsIgnoreCase("attribute")){
			sb.append("My attribute is: " + this.attribute + "\n");
		}
		if(this.operandType.equalsIgnoreCase("relatedEvent")){
			sb.append("My relevant event is: " + this.relatedState + "\n");
			sb.append("My attribute is: " + this.attribute + "\n");
		}
		if(this.operandType.equalsIgnoreCase("aggregation")){
			sb.append("My aggregation is: " + this.aggregation + "\n");
			sb.append("My relevant event is: " + this.relatedState + "\n");
			sb.append("My attribute is: " + this.attribute + "\n");
		}
		return sb.toString();
	}
	/**
	 * @return the operandType
	 */
	public String getOperandType() {
		return operandType;
	}
	/**
	 * @param operandType the operandType to set
	 */
	public void setOperandType(String operandType) {
		this.operandType = operandType;
	}
	/**
	 * @return the originalRepresentation
	 */
	public String getOriginalRepresentation() {
		return originalRepresentation;
	}
	/**
	 * @param originalRepresentation the originalRepresentation to set
	 */
	public void setOriginalRepresentation(String originalRepresentation) {
		this.originalRepresentation = originalRepresentation;
	}
	/**
	 * @return the formatedRepresentation
	 */
	public String getFormatedRepresentation() {
		return formatedRepresentation;
	}
	/**
	 * @param formatedRepresentation the formatedRepresentation to set
	 */
	public void setFormatedRepresentation(String formatedRepresentation) {
		this.formatedRepresentation = formatedRepresentation;
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
	 * @return the aggregation
	 */
	public String getAggregation() {
		return aggregation;
	}
	/**
	 * @param aggregation the aggregation to set
	 */
	public void setAggregation(String aggregation) {
		this.aggregation = aggregation;
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
	 * @return the isNumber
	 */
	public boolean isNumber() {
		return isNumber;
	}
	/**
	 * @param isNumber the isNumber to set
	 */
	public void setNumber(boolean isNumber) {
		this.isNumber = isNumber;
	}
	/**
	 * @return the hasRelatedState
	 */
	public boolean isHasRelatedState() {
		return hasRelatedState;
	}
	/**
	 * @param hasRelatedState the hasRelatedState to set
	 */
	public void setHasRelatedState(boolean hasRelatedState) {
		this.hasRelatedState = hasRelatedState;
	}
	/**
	 * @return the hasAggregation
	 */
	public boolean isHasAggregation() {
		return hasAggregation;
	}
	/**
	 * @param hasAggregation the hasAggregation to set
	 */
	public void setHasAggregation(boolean hasAggregation) {
		this.hasAggregation = hasAggregation;
	}
	/**
	 * @return the isSingle
	 */
	public boolean isSingle() {
		return isSingle;
	}
	/**
	 * @param isSingle the isSingle to set
	 */
	public void setSingle(boolean isSingle) {
		this.isSingle = isSingle;
	}
	
	

}
