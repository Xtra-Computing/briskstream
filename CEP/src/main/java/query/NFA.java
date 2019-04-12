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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import engine.ConfigFlags;
import engine.Run;


/**
 * This class represents an NFA.
 * @author haopeng
 *
 */
public class NFA {
	/**
	 * The states
	 */
	State[] states;
	
	/**
	 * The number of states
	 */
	int size = 0; 
	
	/**
	 * The selection strategy
	 */
	String selectionStrategy;
	
	/**
	 * The time window
	 */
	int timeWindow;
	
	/**
	 * Flag denoting whether the query needs value vector
	 * It is needed when there are aggregates or parameterized predicates
	 */
	boolean needValueVector;
	
	/**
	 * The value vectors for the computation state
	 */
	ValueVectorTemplate[][] valueVectors;
	
	/**
	 * Denoting whether the query has value vectors
	 */
	boolean hasValueVector[];
	
	/**
	 * Specifies the partiton attribute, only for partiton-contiguity selection strategy.
	 */
	String partitionAttribute;// this is used only when we use partition-contiguity selection strategy
	/**
	 * Flag denoting whether the query has a partition attribute
	 */
	boolean hasPartitionAttribute;
	/**
	 * Store other partition attributes except for the first one, if any
	 */
	ArrayList<String> morePartitionAttribute;
	/**
	 * Flag denoting whether the query has more than one partition attributes
	 */
	boolean hasMorePartitionAttribute;
	/**
	 * Flag denoting whether the query has a negation component
	 */
	boolean hasNegation;
	/**
	 * The negation state
	 */
	State negationState;
	
	/**
	 * Constructs an NFA from a file
	 * @param nfaFile the nfa file
	 */
	public NFA(String nfaFile){
		parseNfaFile(nfaFile);
		this.testNegation();
		this.compileValueVectorOptimized();
		
	}
	
	/**
	 * Constructs an NFA from a file, specifies the selection strategy
	 * @param selectionStrategy
	 * @param nfaFile
	 */
	public NFA(String selectionStrategy, String nfaFile){
		this.selectionStrategy = selectionStrategy;
		parseNfaFile(nfaFile);
		this.compileValueVectorOptimized();
	}
	
	/**
	 * Parses the nfa file
	 * @param nfaFile the nfa file
	 */
	public void parseNfaFile(String nfaFile) {
		String line = "";
		try {
			BufferedReader br = new BufferedReader(new FileReader(nfaFile));
			line = br.readLine();
			if(line.startsWith("SelectionStrategy") || line.startsWith("selectionStrategy")){
			// parse the descriptive format
			// next line, parse the configuration parameters
			parseNfaConfig(line);
			// count the size of nfa
			while ((line = br.readLine())!=null){
				if (line.equalsIgnoreCase("end"))
					break;
				else
					size ++;
			}			
			states = new State[size];
			
			//parse each state			
			br = new BufferedReader(new FileReader(nfaFile));
			br.readLine();// pass the configuration line;
			
			int counter = 0;
			while((line = br.readLine())!=null){
				
				if (line.equalsIgnoreCase("end")) // reads the end of nfa file
					break;
				else 
				{
					states[counter] = new State(line.trim(), counter);// starts with 0
					counter ++;
				}	
				
			}
			}
			else if(line.startsWith("PATTERN")){
				// parse the simpler format
				this.morePartitionAttribute = new ArrayList<String>();
				this.hasMorePartitionAttribute = false;
				do {
					this.parseFastQueryLine(line);
				}
				while((line = br.readLine()) != null);
				if(this.hasMorePartitionAttribute){
					this.addMorePartitionAttribute();
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(this.size > 0){
		states[0].setStart(true);
		states[size-1].setEnding(true);
		}
	}
	/**
	 * Parses each line for the fast query format
	 * @param line
	 */
	public void parseFastQueryLine(String line){
		if(line.startsWith("PATTERN")){
			this.parseFastQueryLineStartWithPattern(line);
		}else if(line.startsWith("WHERE")){
			// parse the selection strategy
			StringTokenizer st = new StringTokenizer(line);
			st.nextToken();
			this.selectionStrategy = st.nextToken().trim();
		}else if(line.startsWith("AND")){
			this.parseFastQueryLineStartWithAND(line);
		}else if(line.startsWith("WITHIN")){
			// parse the time window
			StringTokenizer st = new StringTokenizer(line);
			st.nextToken();
			this.timeWindow = Integer.parseInt(st.nextToken().trim());
		}
	}
	/**
	 * Parses the query sequence
	 * @param line
	 */
	public void parseFastQueryLineStartWithPattern(String line){
		String seq = line.substring(line.indexOf('(') + 1, line.indexOf(')'));
		StringTokenizer st = new StringTokenizer(seq, ",");
		this.size = st.countTokens();
		//System.out.println(size);
		this.states = new State[size];
		String state;
		for(int i = 0; i < size; i ++){
			boolean isKleeneClosure = false;
			boolean isNegation = false;
			state = st.nextToken();
			
			StringTokenizer stateSt = new StringTokenizer(state);
			String eventType = stateSt.nextToken().trim();
			String stateTag = stateSt.nextToken().trim();
			if(eventType.contains("+")){
				isKleeneClosure = true;
				eventType = eventType.substring(0, eventType.length() - 1);// the first letter
				stateTag = stateTag.substring(0, 1);
			}else if(eventType.contains("!")){
				isNegation = true;
				eventType = eventType.substring(1, eventType.length());
				
			}
			
			//System.out.println("The tag for state " + i + " is " + stateTag);
			if(isKleeneClosure){
				
				this.states[i] = new State(i + 1, stateTag, eventType, "kleeneClosure");
			}else if(isNegation){
				this.states[i] = new State(i + 1, stateTag, eventType, "negation");
			}else{
				this.states[i] = new State (i + 1, stateTag, eventType, "normal");
			}
		}
	}
	/**
	 * Parses the conditions starting with "AND", it might be the partition attribute, or predicates for states
	 * @param line
	 */
	public void parseFastQueryLineStartWithAND(String line){
		StringTokenizer st = new StringTokenizer(line);
		st.nextToken();
		String token = st.nextToken().trim();  
		if(token.startsWith("[")){
			//the partition attribute
			if(!this.hasPartitionAttribute){
			this.partitionAttribute = token.substring(1, token.length() - 1);
			this.hasPartitionAttribute = true;
			}else{
				this.hasMorePartitionAttribute = true;
				this.morePartitionAttribute.add(token.substring(1, token.length() - 1));
			}
		}else {
			char initial = token.charAt(0);
			int stateNum = initial - 'a';//determine which state this predicate works for according to the initial
			this.states[stateNum].addPredicate(line.substring(3).trim());
		}
		//todo for states
		
	}
	/**
	 * Adds other partition attributes except for the first to each state
	 */
	public void addMorePartitionAttribute(){
		String tempPredicate;
		for(int i = 0; i < this.morePartitionAttribute.size(); i ++){
			tempPredicate = this.morePartitionAttribute.get(i)+"=$1." + this.morePartitionAttribute.get(i);//?
			for(int j = 1; j < this.size; j ++){
				State tempState = this.getStates(j);
				for(int k = 0; k < tempState.getEdges().length; k ++){
					Edge tempEdge = tempState.getEdges(k);
					tempEdge.addPredicate(tempPredicate);
				}
			}
		}
	}
	/**
	 * Parses the configuration line in the nfa file
	 * @param line
	 */
	public void parseNfaConfig(String line){
		StringTokenizer st = new StringTokenizer(line, "|");
		while(st.hasMoreTokens()){
			parseConfig(st.nextToken().trim());
		}
		
	}
	
	/**
	 * Parses a configuration, now we have selection strategy, time window and partiton attribute
	 * @param attribute a configuration
	 */
	public void parseConfig(String attribute){
		StringTokenizer st = new StringTokenizer(attribute, "=");
		String left = st.nextToken().trim();
		String right = st.nextToken().trim();
		if(left.equalsIgnoreCase("selectionStrategy")){
			this.selectionStrategy = right;
		}else if(left.equalsIgnoreCase("timeWindow")){
			this.timeWindow = Integer.parseInt(right);
		}else if(left.equalsIgnoreCase("partitionAttribute")){
			this.partitionAttribute = right;
			ConfigFlags.partitionAttribute = right;
			this.hasPartitionAttribute = true;
		}
	}
	/**
	 * Tests whether the query contains a negation component
	 */
	public void testNegation(){
		
		for(int i = 0; i < this.size; i ++){
			if(this.getStates(i).stateType.equalsIgnoreCase("negation")){
				this.hasNegation = true;
				this.negationState = this.getStates(i);
			}
		}
		if(this.hasNegation){
		int negationOrder = this.negationState.getOrder()- 1;
		State newState[] = new State[this.size - 1];
		for(int i = 0; i < this.size - 1; i ++){
			if(i < negationOrder){
				newState[i] = this.getStates(i);
				if(i == negationOrder - 1){
					newState[i].setBeforeNegation(true);
				}
			}else {
				newState[i] = this.getStates(i + 1);
				if(i == negationOrder ){
					newState[i].setAfterNegation(true);
				}
			
			}
		}
		this.size = this.size - 1;
		this.setStates(newState);
		}
		
	}
	
	/**
	 * Compiles the value vector based on the nfa
	 */
	
	public void compileValueVectorOptimized(){
		this.valueVectors = new ValueVectorTemplate[this.size][];
		ArrayList<ValueVectorTemplate> valueV = new ArrayList<ValueVectorTemplate>();
		int counter[] = new int[this.size];
		for(int i = 0; i < this.size; i ++){
			counter[i] = 0;
		}
		for(int i = 0; i < this.getSize(); i ++){
			State tempState = this.getStates(i);
			
			for(int j = 0; j < tempState.getEdges().length; j ++){
				Edge tempEdge = tempState.getEdges(j);
				for(int k = 0; k < tempEdge.getPredicates().length; k ++){
					PredicateOptimized tempPredicate = tempEdge.getPredicates()[k];
					if(!tempPredicate.isSingleState()){
						String operationName = tempPredicate.getOperation();
						String attributeName = tempPredicate.getAttributeName();
						int stateNumber;
						if(tempPredicate.getRelatedState().equals("previous")){
							stateNumber = i - 1;
						}else{
							stateNumber = Integer.parseInt(tempPredicate.getRelatedState()) - 1;
						}
						valueV.add(new ValueVectorTemplate(stateNumber,attributeName, operationName,i));
						counter[stateNumber]++;
				
				}
			}
		}
						
		}
		
		//set the needValueVector flag as true
		if(valueV.size() > 0){
			this.needValueVector = true;
		}
		//put the value vector tempate to each state
		for(int i = 0; i < this.size ; i ++){
			this.valueVectors[i] = new ValueVectorTemplate[counter[i]];
			ValueVectorTemplate temp;
			int count = 0;
			for(int j = 0; j < valueV.size(); j ++){
				temp = valueV.get(j);
				if(temp.getState() == i){
					this.valueVectors[i][count] = temp;
					count ++;
				}
			}
		}
		this.hasValueVector = new boolean[this.size];
		for(int i = 0; i < this.size; i ++){
			if(counter[i]>0){
				this.hasValueVector[i] = true;
			}else{
				this.hasValueVector[i] = false;
			}
		}
		
		
		
	}
	

	/**
	 * Self description
	 */
	public String toString(){
		String temp = "";
		//temp += "I am an NFA";
		temp += "The selection strategy is: " + this.selectionStrategy;
		temp += "\nThe time window is : " + this.timeWindow;
		if(this.size > 0){
		temp += "\nThere are " + this.getStates().length + " states\n";
		for(int i = 0; i < this.getStates().length; i ++){
			temp += ("NO." + i + " state:" + this.getStates(i));
		}
		}
		if(this.hasPartitionAttribute == true){
			System.out.println("The partition attribute is: " + this.partitionAttribute);
		}
		return temp;
	}

	
	/**
	 * @return the states
	 */
	public State[] getStates() {
		return states;
	}
	public State getStates(int order) {
		// for debug
		/*
		if(order == 3){
			System.out.println();
		}
		*/
		return states[order];
	}
	/**
	 * @param states the states to set
	 */
	public void setStates(State[] states) {
		this.states = states;
	}
	/**
	 * @return the size
	 */
	public int getSize() {
		return size;
	}
	/**
	 * @param size the size to set
	 */
	public void setSize(int size) {
		this.size = size;
	}
	/**
	 * @return the selectionStrategy
	 */
	public String getSelectionStrategy() {
		return selectionStrategy;
	}
	/**
	 * @param selectionStrategy the selectionStrategy to set
	 */
	public void setSelectionStrategy(String selectionStrategy) {
		this.selectionStrategy = selectionStrategy;
	}

	/**
	 * @return the timeWindow
	 */
	public int getTimeWindow() {
		return timeWindow;
	}

	/**
	 * @param timeWindow the timeWindow to set
	 */
	public void setTimeWindow(int timeWindow) {
		this.timeWindow = timeWindow;
	}

	/**
	 * @return the needValueVector
	 */
	public boolean isNeedValueVector() {
		return needValueVector;
	}

	/**
	 * @param needValueVector the needValueVector to set
	 */
	public void setNeedValueVector(boolean needValueVector) {
		this.needValueVector = needValueVector;
	}



	/**
	 * @return the partitionAttribute
	 */
	public String getPartitionAttribute() {
		return partitionAttribute;
	}

	/**
	 * @param partitionAttribute the partitionAttribute to set
	 */
	public void setPartitionAttribute(String partitionAttribute) {
		this.partitionAttribute = partitionAttribute;
	}

	/**
	 * @return the valueVectors
	 */
	public ValueVectorTemplate[][] getValueVectors() {
		return valueVectors;
	}

	/**
	 * @param valueVectors the valueVectors to set
	 */
	public void setValueVectors(ValueVectorTemplate[][] valueVectors) {
		this.valueVectors = valueVectors;
	}

	/**
	 * @return the hasValueVector
	 */
	public boolean[] getHasValueVector() {
		return hasValueVector;
	}

	/**
	 * @param hasValueVector the hasValueVector to set
	 */
	public void setHasValueVector(boolean[] hasValueVector) {
		this.hasValueVector = hasValueVector;
	}

	/**
	 * @return the hasPartitionAttribute
	 */
	public boolean isHasPartitionAttribute() {
		return hasPartitionAttribute;
	}

	/**
	 * @param hasPartitionAttribute the hasPartitionAttribute to set
	 */
	public void setHasPartitionAttribute(boolean hasPartitionAttribute) {
		this.hasPartitionAttribute = hasPartitionAttribute;
	}

	/**
	 * @return the hasNegation
	 */
	public boolean isHasNegation() {
		return hasNegation;
	}

	/**
	 * @param hasNegation the hasNegation to set
	 */
	public void setHasNegation(boolean hasNegation) {
		this.hasNegation = hasNegation;
	}

	/**
	 * @return the negationState
	 */
	public State getNegationState() {
		return negationState;
	}

	/**
	 * @param negationState the negationState to set
	 */
	public void setNegationState(State negationState) {
		this.negationState = negationState;
	}
	
	
	
	
}
