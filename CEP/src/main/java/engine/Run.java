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

import edu.umass.cs.sase.query.NFA;
import edu.umass.cs.sase.query.ValueVectorTemplate;
import edu.umass.cs.sase.stream.Event;

/**
 * This class represents a run.
 * @author haopeng
 *
 */
public class Run  implements Cloneable{
	/**
	 * The event ids selected by the run
	 */
	ArrayList<Integer> eventIds;
	/**
	 * The partition id of the run, used under partition-contiguity selection strategy or when a partition attribute is used in other selection strategies
	 */
	int partitonId;
	/**
	 * Flags of the state status
	 * 0 represents for no event, 1 represents for there is event for this state, but predicates has not been finished. 
	 * 2 represents all is done.
	 * 3 represents for kleene closure state, this position already has some event/events, but it can accept new events.
	 */
	int[] state; 
	/**
	 * The number of states
	 */
	int size; 
	/**
	 * The number of events selected so far
	 */
	int count; 
	/**
	 * The nfa for the query
	 */
	NFA nfa;
	/**
	 * Flag denoting whether the run is alive
	 */
	boolean alive;
	/**
	 * Which state the run is at
	 * current state describes where the run is at, this value is initialized as 0, every time we add an event, 
	 * we will increase the current state by 1 except for when the current state is a kleene closure state.
	 * When this run is full, we will not increase it anymore.
	 */
	int currentState;
	/**
	 * Flag denoting whether the run is ready to make a match
	 */
	boolean isComplete;
	/**
	 * The run is full of events, but maybe some predicates has not been evaluated
	 */
	boolean isFull;
	/**
	 * The starting time of this run
	 */
	
	long lifeTimeBegin;
	/**
	 * The timestamp of the first event selected by this run
	 */
	int startTimeStamp;
	/**
	 * Flag denoting whether the kleene closure state in the query (if it contains) is initialized
	 */
	boolean kleeneClosureInitialized;
	
	/**
	 * Value vectors, records the values needed by computation state.
	 */
	
	ValueVectorElement[][] valueVector;
	
	int beforeNegationTimestamp;
	int afterNegationTimestamp;
	/**
	 * Default constructor.
	 */
	public Run(){
		this.alive = false;
		
	}
	/**
	 * Constructor, initialized by the nfa for a query
	 * @param nfa the nfa for a query
	 */
	public Run(NFA nfa){
		this.initializeRun(nfa);
	}
	/**
	 * Initializes a run, sets it as alive
	 * @param nfa the nfa for a query
	 */
	void initializeRun(NFA nfa){
		this.nfa = nfa;
		this.size = this.nfa.getStates().length;
		this.setLifeTimeBegin(System.nanoTime());
		state = new int[size];	
		this.eventIds = new ArrayList<Integer>();		
		currentState = 0;
		this.alive = true;
		this.isFull = false;
		this.isComplete = false;
		this.count = 0;
		this.kleeneClosureInitialized = false;
		if(this.nfa.isNeedValueVector()){
			this.valueVector = new ValueVectorElement[this.size][];
		}
		
	}
	/**
	 * resets a run
	 */
	void resetRun(){
		if(this.nfa.isNeedValueVector()){
			this.valueVector = new ValueVectorElement[this.size][];
		}
		this.nfa = null;
		this.eventIds = null;
		this.state = null;
		this.alive = false;
		this.currentState = -1;
		this.isComplete = false;
		this.isFull = false;
		this.count = 0;
		this.kleeneClosureInitialized = false;
		
	}
	/**
	 * Checks whether the match is ready to make a match
	 * @return the check result, boolean format
	 */
	public boolean checkMatch(){
		if(!this.isFull)
			return false;
		for(int i = 0; i < state.length; i ++){
			if (state[i] != 2){
				return false;
			}
		}
		this.isComplete = true;
		return true;
	}
	/**
	 * Adds an event to the run, and makes necessary updates of the run status and value vectors.
	 * @param e the event to be added
	 */
	public void addEvent(Event e){//1 incomplete, 2 complete ,3 kleene closure
		String stateType = this.nfa.getStates()[currentState].getStateType();
		if(stateType.equalsIgnoreCase("normal")){
			this.eventIds.add(e.getId());
			state[currentState] = 2;
			this.count ++;
			if(currentState == this.nfa.getSize() - 1){
				this.setFull(true);
			}else
				{	
					if(this.nfa.isNeedValueVector()	){
						if(this.nfa.getHasValueVector()[this.currentState]){
							this.initializeValueVector(e);
						}
					}
					this.currentState ++;					
				}
		}else if(stateType.equalsIgnoreCase("kleeneClosure")){
			this.eventIds.add(e.getId());
			if(this.nfa.isNeedValueVector()){
				if(this.nfa.getHasValueVector()[this.currentState]){
					if(this.kleeneClosureInitialized){
						this.updateValueVector(e);
					}else{
						this.initializeValueVector(e);
					}
				}
			}
			this.kleeneClosureInitialized = true;
			state[currentState] = 3;
			this.count ++;			
		}		
		if(this.count == 1){
			this.startTimeStamp = e.getTimestamp();
			if(ConfigFlags.hasPartitionAttribute){
				this.partitonId = e.getAttributeByName(this.nfa.getPartitionAttribute());
			}
		}// if this is the first event of this run, initialize the start timestamp;
	}
/**
 * 
 * @return the id of last selected event
 */
	public int getPreviousEventId(){
		return this.eventIds.get(this.count - 1);
	}
	
	/**
	 * Returns the last n event id
	 * last 1 means the last event
	 * last 2 means the last second event
	 * @param n the relative position
	 * @return the event id
	 */
	public int getLastNEventId(int n){
		if (n > this.count){
			return -1;
		}
		return this.eventIds.get(this.count - n);
	}
	
	
	
	/**
	 * Proceeds a kleene closure state
	 */
	public void proceed(){
		this.state[this.currentState] = 2;
		if(this.currentState == this.size -1){
			this.setFull(true);
			//this.checkMatch();??
		}else{
			this.currentState ++;
		}
	}
	/**
	 * Clones the run itself
	 */
	public Object clone() throws CloneNotSupportedException{
		
		Run o = null;
		o = (Run)super.clone();
		o.setEventIds((ArrayList<Integer>)this.getEventIds().clone());
		o.setState((int[])this.getState().clone());
		o.setNfa(nfa);
		return o;
	}
	/**
	 * Initializes the value vectors of the computation state
	 * @param e the first selected event for a state
	 */	
	public void initializeValueVector(Event e){
		ValueVectorTemplate[] temp = this.nfa.getValueVectors()[this.currentState];
		this.valueVector[this.currentState] = new ValueVectorElement[temp.length];
		for(int i = 0; i < temp.length; i ++){
			String opr = temp[i].getOperation();
			if(opr.equalsIgnoreCase("avg")){
				this.valueVector[this.currentState][i] = new ValueVectorElementAvg();
			}else if(opr.equalsIgnoreCase("max")){
				this.valueVector[this.currentState][i] = new ValueVectorElementMax();
			}else if(opr.equalsIgnoreCase("min")){
				this.valueVector[this.currentState][i] = new ValueVectorElementMin();
			}else if(opr.equalsIgnoreCase("set")){
				this.valueVector[this.currentState][i] = new ValueVectorElementSet();
			}else if(opr.equalsIgnoreCase("count")){
				this.valueVector[this.currentState][i] = new ValueVectorElementCount();
			}else if(opr.equalsIgnoreCase("sum")){
				this.valueVector[this.currentState][i] = new ValueVectorElementSum();
			}
			this.valueVector[this.currentState][i].setAttribute(temp[i].getAttribute());
			this.valueVector[this.currentState][i].setNeededByState(temp[i].getNeededByState());
			this.valueVector[this.currentState][i].initializeValue(e);
			
		}
	}
	/**
	 * Updates the value vectors for the computation state
	 * @param e the latest selected event
	 */
	public void updateValueVector(Event e){
		for(int i = 0; i < this.valueVector[this.currentState].length; i ++){
			this.valueVector[this.currentState][i].updateValue(e);
		}
	}
	/**
	 * returns the needed value vector
	 * @param stateNumber the state which the needed value vector is at
	 * @param attribute the attribute of the value vector
	 * @param operation the operation name of the value vector
	 * @return the current value of the value vector
	 */
	public int getNeededValueVector(int stateNumber, String attribute, String operation){
		for(int i = 0; i < this.valueVector[stateNumber].length; i ++){
			String att = this.valueVector[stateNumber][i].getAttribute();
			if(this.valueVector[stateNumber][i].getAttribute().equals(attribute) && 
					this.valueVector[stateNumber][i].getType().equalsIgnoreCase(operation)){
				return this.valueVector[stateNumber][i].getValue();
			}
		}
		return 0;
	}
	
	/**
	 * @return the alive
	 */
	public boolean isAlive() {
		return alive;
	}
	/**
	 * @param alive the alive to set
	 */
	public void setAlive(boolean alive) {
		this.alive = alive;
		if (alive){// if the run is used again, we need to update the beginning of lifetime
			this.lifeTimeBegin = System.nanoTime();
		}
	}
	/**
	 * @return the currentState
	 */
	public int getCurrentState() {
		return currentState;
	}
	/**
	 * @param currentState the currentState to set
	 */
	public void setCurrentState(int currentState) {
		this.currentState = currentState;
	}
	/**
	 * @return the state
	 */
	public int[] getState() {
		return state;
	}
	/**
	 * @param state the state to set
	 */
	public void setState(int[] state) {
		this.state = state;
	}
	/**
	 * @return the nfa
	 */
	public NFA getNfa() {
		return nfa;
	}
	/**
	 * @param nfa the nfa to set
	 */
	public void setNfa(NFA nfa) {
		this.nfa = nfa;
	}
	/**
	 * @return the lifeTimeBegin
	 */
	public long getLifeTimeBegin() {
		return lifeTimeBegin;
	}
	/**
	 * @param lifeTimeBegin the lifeTimeBegin to set
	 */
	public void setLifeTimeBegin(long lifeTimeBegin) {
		this.lifeTimeBegin = lifeTimeBegin;
	}
	/**
	 * @return the isComplete
	 */
	public boolean isComplete() {
		return isComplete;
	}

	/**
	 * @param isComplete the isComplete to set
	 */
	public void setComplete(boolean isComplete) {
		this.isComplete = isComplete;
	}

	/**
	 * @return the isFull
	 */
	public boolean isFull() {
		return isFull;
	}

	/**
	 * @param isFull the isFull to set
	 */
	public void setFull(boolean isFull) {
		this.isFull = isFull;
	}

	/**
	 * @return the startTimeStamp
	 */
	public int getStartTimeStamp() {
		return startTimeStamp;
	}

	/**
	 * @param startTimeStamp the startTimeStamp to set
	 */
	public void setStartTimeStamp(int startTimeStamp) {
		this.startTimeStamp = startTimeStamp;
	}

	/**
	 * @return the eventIds
	 */
	public ArrayList<Integer> getEventIds() {
		return eventIds;
	}

	/**
	 * @param eventIds the eventIds to set
	 */
	public void setEventIds(ArrayList<Integer> eventIds) {
		this.eventIds = eventIds;
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
	 * @return the count
	 */
	public int getCount() {
		return count;
	}

	/**
	 * @param count the count to set
	 */
	public void setCount(int count) {
		this.count = count;
	}



	/**
	 * @return the partitonId
	 */
	public int getPartitonId() {
		return partitonId;
	}

	/**
	 * @param partitonId the partitonId to set
	 */
	public void setPartitonId(int partitonId) {
		this.partitonId = partitonId;
	}

	/**
	 * @return the kleeneClosureInitialized
	 */
	public boolean isKleeneClosureInitialized() {
		return kleeneClosureInitialized;
	}

	/**
	 * @param kleeneClosureInitialized the kleeneClosureInitialized to set
	 */
	public void setKleeneClosureInitialized(boolean kleeneClosureInitialized) {
		this.kleeneClosureInitialized = kleeneClosureInitialized;
	}

	/**
	 * @return the valueVector
	 */
	public ValueVectorElement[][] getValueVector() {
		return valueVector;
	}

	/**
	 * @param valueVector the valueVector to set
	 */
	public void setValueVector(ValueVectorElement[][] valueVector) {
		this.valueVector = valueVector;
	}
	/**
	 * @return the beforeNegationTimestamp
	 */
	public int getBeforeNegationTimestamp() {
		return beforeNegationTimestamp;
	}
	/**
	 * @param beforeNegationTimestamp the beforeNegationTimestamp to set
	 */
	public void setBeforeNegationTimestamp(int beforeNegationTimestamp) {
		this.beforeNegationTimestamp = beforeNegationTimestamp;
	}
	/**
	 * @return the afterNegationTimestamp
	 */
	public int getAfterNegationTimestamp() {
		return afterNegationTimestamp;
	}
	/**
	 * @param afterNegationTimestamp the afterNegationTimestamp to set
	 */
	public void setAfterNegationTimestamp(int afterNegationTimestamp) {
		this.afterNegationTimestamp = afterNegationTimestamp;
	}
	

}
