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



import net.sourceforge.jeval.EvaluationException;
import stream.Event;

/**
 * This class represents a state from NFA.
 * @author haopeng
 * 
 */
public class State {
	

	/**
	 * The line in the nfa file for this state
	 */
	String nfaLine;
	
	/**
	 * The type of the state, normal, kleene closure or negation
	 */
	String stateType; 
	
	/**
	 * The event type for this state
	 */
	String eventType;
	
	/**
	 * The order of this state
	 */
	int order;
	
	/**
	 * Denoting whether this state is the first state
	 */
	boolean isStart;
	
	/**
	 * Denoting whether this state is the last state
	 */
	boolean isEnding;
	
	/**
	 * Denoting whether this state is a kleene closure state
	 */
	boolean isKleeneClosure;
	
	boolean isNegation;
	boolean isBeforeNegation;
	boolean isAfterNegation;
	
	/**
	 * The edges from this state
	 */
	Edge[] edges;
	
	/**
	 * Representation for this state in the fast query format, usually 'a' for the first state, 'b' for the second state, etc.
	 */
	String tag;
	
	/**
	 * Constructs a state based on a line in the nfa file, and the order
	 * @param nfaLine the line in the nfa file
	 * @param order the order of this state
	 */
	public State(String nfaLine, int order){
		this.nfaLine = nfaLine;
		this.order = order;
		isStart = false;
		isEnding = false;
		isKleeneClosure = false;
		eventType = "test";
		parseNfaLine(nfaLine);
		if(this.stateType.equalsIgnoreCase("kleeneclosure")){
			this.isKleeneClosure = true;
		}
		
	}
	public State(int order, String tag, String eventType, String stateType){
		this.order = order;
		this.tag = tag;
		this.eventType = eventType;
		this.stateType = stateType;
		if(this.stateType.equalsIgnoreCase("normal")){
			this.isKleeneClosure = false;
			this.isNegation = false;
			this.edges = new Edge[1];
			this.edges[0] = new Edge(0);
			
		}else if(this.stateType.equalsIgnoreCase("kleeneClosure")){
			this.isKleeneClosure = true;
			this.isNegation = false;
			this.edges = new Edge[3];
			for(int i = 0; i < 3; i ++){
				this.edges[i] = new Edge(i);
			}
		}else if(this.stateType.equalsIgnoreCase("negation")){
			this.isKleeneClosure = false;
			this.isNegation = true;
			this.edges = new Edge[1];
			this.edges[0] = new Edge(0);
		}
		
	}
	/**
	 * Adds a predicate to this state based on the given description.
	 * @param pDescription
	 */
	public void addPredicate(String pDescription){
		//System.out.println("A new predicate is added to this state: " + this.tag);
		//System.out.println(pDescription);
		// a predicate should be composed of 3 parts: left operator right
		StringTokenizer st = new StringTokenizer(pDescription);
		int size = st.countTokens();
		String left = st.nextToken();
		
		int edgeNumber = this.parseEdgeNumber(left);
		//System.out.println("left=" + left);
		
		String right = null;
		while(st.hasMoreTokens()){
			right = st.nextToken();}
		
		//System.out.println("right=" + right);
		String newLeft = this.replaceLeftStateNumber(left);
		String newRight = this.replaceRightStateNumber(right);
		
		
		String p = pDescription.replace(left, newLeft);
		p = p.replace(right, newRight);
		this.edges[edgeNumber].addPredicate(p);
		//System.out.println("predicate after parsing is:" + p);
		
		
	}
	/**
	 * Used to replace the state number of the left operand
	 * @param original
	 * @return the replaced string
	 */
	public String replaceLeftStateNumber(String original){
		int dotPosition = original.indexOf('.');
		return original.substring(dotPosition + 1, original.length());
	}
	/**
	 * Used to replace the state number of the right operand
	 * @param original
	 * @return the replaced string
	 */
	public String replaceRightStateNumber(String original){
		if(original.contains("(")){
			String innerPart = this.parseRightStateNumber(original.substring(original.indexOf('(') + 1,original.indexOf(')')));
			String outterPart = original.substring(0, original.indexOf('('));
			return outterPart + "(" + innerPart + ")";
		}else{
			return this.parseRightStateNumber(original);
		}
	
	}
	/**
	 * Parses the state number of the right operand
	 * @param original
	 * @return the parsed state number
	 */
	public String parseRightStateNumber(String original){
		if(!original.contains(".")){
			return original;
		}else{
			if(original.contains("[..i-1]")){
				char initial = original.charAt(0);
				String stateNamePart = original.substring(0, original.indexOf(']')+1);
				return original.replace(stateNamePart,	"$" + (initial - 'a' + 1));
			}
			String stateName = original.substring(0, original.indexOf('.'));
			if(stateName.length() == 1){
				if(stateName.equalsIgnoreCase(this.tag)){
					return original.substring(original.indexOf('.'));
				}else{
					return original.replace(stateName, "$" + (stateName.charAt(0) - 'a' + 1));
				}
			}else{
				return "$previous" + original.substring(original.indexOf('.'));
			}
		}
		
	}
	/**
	 * Judges the edge type, "take" or "begin"
	 * @param predicateLeft
	 * @return 1 for "take", 0 for "begin"
	 */
	public int parseEdgeNumber(String predicateLeft){
		 if(predicateLeft.contains("[i]")){
			 //"take" edge
			return 1;
		}else
			return 0;
	}
	/**
	 * Parses a line in the nfa file, e.g.: State=1 & type = normal & eventtype = c | edgetype = begin & price < 100
	 * @param nfaLine the line in the nfa file
	 */
	void parseNfaLine(String nfaLine){
		this.nfaLine = nfaLine;
		StringTokenizer st = new StringTokenizer(nfaLine, "|"); //parse the state and edges
		int count = 0;
		int size = st.countTokens();//count the size of edges
		edges = new Edge[size - 1];// parse the edge
		while (st.hasMoreTokens()){
			if(count == 0){// parse the part describing the state
				parseState(st.nextToken().trim());
				count ++;
			}
			else{
				edges[count -1] = new Edge(st.nextToken().trim());
				count ++;
			}
		}
	}
	
	/**
	 * Parses the description for state, e.g.: state = 1, type = normal/kleeneclosure/negation
	 * @param stateLine the description for this state
	 */
	public void parseState(String stateLine){ 
		StringTokenizer st = new StringTokenizer(stateLine, "&");
		while (st.hasMoreTokens()){
			parseEquation(st.nextToken().trim());
		}
	}
	
	/**
	 * Parses the formulas in the query, e.g.: price > 100
	 * @param equation the formula string
	 */
	public void parseEquation(String equation){// parse the state/state type, eventtype
		StringTokenizer st = new StringTokenizer(equation, "=");
		String left = st.nextToken().trim();
		String right = st.nextToken().trim();
		if (left.equalsIgnoreCase("state")){
			this.order = Integer.parseInt(right);
		} else if(left.equalsIgnoreCase("type"))
		{
			this.stateType = right;
		}else if(left.equalsIgnoreCase("eventtype")){
			this.eventType = right;
		}
	}
	
	/**
	 * Self description
	 */
	public String toString(){
		String temp = "";
		if(isStart)
			temp += " I am a starting state";
		if(isEnding)
			temp += " I am a ending state";
		if(isKleeneClosure)
			temp += " I am a kleene closure state";
		temp += " My state type is: " + this.stateType;
		temp += "\n my description file = " + this.nfaLine;
		return "This is the " + order + " state, requiring events of " + eventType
			+" event type, "+ temp;
	}

	public boolean canStartWithEvent(Event e) throws EvaluationException{
	
		
		if(!e.getEventType().equalsIgnoreCase(this.eventType)){
			
			return false;
		}
		if(this.edges[0].evaluatePredicate(e,e)){
			return true;
		}
		return false;
		
	}

	
	
	
	
	/**
	 * @return the eventType
	 */
	public String getEventType() {
		return eventType;
	}


	/**
	 * @param eventType the eventType to set
	 */
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}


	/**
	 * @return the order
	 */
	public int getOrder() {
		return order;
	}

	/**
	 * @param order the order to set
	 */
	public void setOrder(int order) {
		this.order = order;
	}

	/**
	 * @return the isStart
	 */
	public boolean isStart() {
		return isStart;
	}


	/**
	 * @param isStart the isStart to set
	 */
	public void setStart(boolean isStart) {
		this.isStart = isStart;
	}


	/**
	 * @return the isEnding
	 */
	public boolean isEnding() {
		return isEnding;
	}


	/**
	 * @param isEnding the isEnding to set
	 */
	public void setEnding(boolean isEnding) {
		this.isEnding = isEnding;
	}


	/**
	 * @return the isKleeneClosure
	 */
	public boolean isKleeneClosure() {
		return isKleeneClosure;
	}


	/**
	 * @param isKleeneClosure the isKleeneClosure to set
	 */
	public void setKleeneClosure(boolean isKleeneClosure) {
		this.isKleeneClosure = isKleeneClosure;
	}
	/**
	 * @return the nfaLine
	 */
	public String getNfaLine() {
		return nfaLine;
	}
	/**
	 * @param nfaLine the nfaLine to set
	 */
	public void setNfaLine(String nfaLine) {
		this.nfaLine = nfaLine;
	}
	/**
	 * @return the stateType
	 */
	public String getStateType() {
		return stateType;
	}
	/**
	 * @param stateType the stateType to set
	 */
	public void setStateType(String stateType) {
		this.stateType = stateType;
	}
	/**
	 * @return the edges
	 */
	public Edge[] getEdges() {
		return edges;
	}
	
	public Edge getEdges(int order) {
		return edges[order];
	}
	/**
	 * @param edges the edges to set
	 */
	public void setEdges(Edge[] edges) {
		this.edges = edges;
	}
	/**
	 * @return the isNegation
	 */
	public boolean isNegation() {
		return isNegation;
	}
	/**
	 * @param isNegation the isNegation to set
	 */
	public void setNegation(boolean isNegation) {
		this.isNegation = isNegation;
	}
	/**
	 * @return the isBeforeNegation
	 */
	public boolean isBeforeNegation() {
		return isBeforeNegation;
	}
	/**
	 * @param isBeforeNegation the isBeforeNegation to set
	 */
	public void setBeforeNegation(boolean isBeforeNegation) {
		this.isBeforeNegation = isBeforeNegation;
	}
	/**
	 * @return the isAfterNegation
	 */
	public boolean isAfterNegation() {
		return isAfterNegation;
	}
	/**
	 * @param isAfterNegation the isAfterNegation to set
	 */
	public void setAfterNegation(boolean isAfterNegation) {
		this.isAfterNegation = isAfterNegation;
	}
	/**
	 * @return the tag
	 */
	public String getTag() {
		return tag;
	}
	/**
	 * @param tag the tag to set
	 */
	public void setTag(String tag) {
		this.tag = tag;
	}
	
	
	
	
}
