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

import engine.EventBuffer;
import engine.Run;
import net.sourceforge.jeval.EvaluationException;
import stream.Event;

/**
 * This class represents an edge of an NFA.
 * @author haopeng
 *
 */
//edgetype = begin & price < 100
public class Edge {
	/**
	 * The original description in the nfa file
	 */
	String edgeDescription;
	/**
	 * The type of the edge, begin, , take or proceed
	 */
	String edgeType;
	
	/**
	 * Predicates on this edge
	 */
	PredicateOptimized predicates[];

	/**
	 * Constructor, based on the input string
	 * @param edgeDescription the description sentence in the nfa file
	 */
	public Edge(String edgeDescription){
		this.edgeDescription = edgeDescription;
		StringTokenizer st = new StringTokenizer(edgeDescription, "&");
		int size = st.countTokens();
		parseEdgeType(st.nextToken().trim());
		
		predicates = new PredicateOptimized[size -1];
		
		int count = 0;
		while(st.hasMoreTokens()){
			predicates[count] = new PredicateOptimized(st.nextToken().trim());
			count ++;
		}		
	}
	public Edge(int edgeTypeNum){
		this.predicates = new PredicateOptimized[0];
		switch(edgeTypeNum){
			case 0: this.edgeType = "begin";
					break;
			case 1: this.edgeType = "take";
					break;
			case 2: this.edgeType = "proceed";
					break;
		}
	}
	public void addPredicate(String predicateDescription){
		
			PredicateOptimized newPredicates[] = new PredicateOptimized[this.predicates.length + 1];
			for(int i = 0; i < this.predicates.length; i ++){
				newPredicates[i] = this.predicates[i];
			}
			newPredicates[newPredicates.length - 1] = new PredicateOptimized(predicateDescription);
			this.predicates = newPredicates;
		
	}
	
	/**
	 * Evaluates an event on this edge
	 * @param currentEvent current event
	 * @param previousEvent the previous event
	 * @return the evaluation result.
	 * @throws EvaluationException
	 */
	public boolean evaluatePredicate(Event currentEvent, Event previousEvent) throws EvaluationException{
		for(int i = 0; i < this.predicates.length; i ++){
			
				if(!this.predicates[i].evaluate(currentEvent, previousEvent)){
					return false;
			
		}}
		return true;
	}
	

	
	/**
	 * Override method, evaluates event based on the current event, and a run.
	 * @param currentEvent the current event
	 * @param r a run
	 * @return the evaluate result
	 */
	public boolean evaluatePredicate(Event currentEvent, Run r, EventBuffer b){
		for(int i = 0; i < this.predicates.length; i ++){
			try {
				if(!this.predicates[i].evaluate(currentEvent, r, b)){
					return false;
				}
			} catch (EvaluationException e) {
				
				e.printStackTrace();
			}
		}
		return true;
	}
	
	/**
	 * Parses the edge from the input string
	 * @param edgeTypeDescription the description string
	 */
	void parseEdgeType(String edgeTypeDescription){
		StringTokenizer st = new StringTokenizer(edgeTypeDescription, "=");
		String left = st.nextToken().trim();
		String right = st.nextToken().trim();
		if(left.equalsIgnoreCase("edgetype"))
			this.edgeType = right;
		
	}
	/**
	 * Self-description
	 */
	public String toString(){
		String temp = "";
		temp += "I am an edge, my edge type is " + this.edgeType;
		temp += "\nmy edge descripton file is " + this.edgeDescription;
		temp += "\nI have " + this.predicates.length + " predicates";
		return temp;
	}

	/**
	 * @return the edgeDescription
	 */
	public String getEdgeDescription() {
		return edgeDescription;
	}

	/**
	 * @param edgeDescription the edgeDescription to set
	 */
	public void setEdgeDescription(String edgeDescription) {
		this.edgeDescription = edgeDescription;
	}

	/**
	 * @return the edgeType
	 */
	public String getEdgeType() {
		return edgeType;
	}

	/**
	 * @param edgeType the edgeType to set
	 */
	public void setEdgeType(String edgeType) {
		this.edgeType = edgeType;
	}

	/**
	 * @return the predicates
	 */
	public PredicateOptimized[] getPredicates() {
		return predicates;
	}

	/**
	 * @param predicates the predicates to set
	 */
	public void setPredicates(PredicateOptimized[] predicates) {
		this.predicates = predicates;
	}
	
}
