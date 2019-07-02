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

import query.NFA;
import stream.Event;


/**
 * This class represents a match.
 * @author haopeng
 *
 */
public class Match {
	/**
	 * The events selected for this match
	 */
	Event[] events;
	/**
	 * The events selected for this match
	 */
	ArrayList<Event> eventList;
	/**
	 * The nfa for the query
	 */
	NFA nfa;
	/**
	 * default constructor
	 */
	public Match(){
		
	}
	/**
	 * Constructor, used by the default engine
	 * @param r the input run
	 * @param nfa the nfa for the query
	 * @param buffer the event buffer
	 */
	public Match(Run r, NFA nfa, EventBuffer buffer){
		
		
		this.events = new Event[r.getCount()];
		for(int i = 0; i < r.getCount(); i ++){
			this.events[i] = buffer.getEvent(r.getEventIds().get(i));
		}
		this.nfa = nfa;

	}
	/**
	 * Constructor, used by the sharing engine
	 * @param eventList the events for this match
	 * @param nfa the nfa for the query
	 */
	public Match(ArrayList<Event> eventList, NFA nfa){
		this.eventList = eventList;
		this.nfa = nfa;
	}
/**
 * outputs the description
 */
	public String toString(){
		String temp = "";
		//temp += "This is a match for this query:\n:";
		//temp += this.nfa.toString();
		temp += "\nThis match has selected the following events: \n\n";
		if(this.events != null){
			for(int i = 0; i < this.events.length; i ++){
				temp += this.events[i].toString() +"\n";
				}
		}else if(this.eventList.size() > 0){
			for(int i = 0; i < this.eventList.size(); i ++){
				temp += this.eventList.get(i).toString() +"\n";
				}
		}

		return temp;
	}

	/**
	 * @return the events
	 */
	public Event[] getEvents() {
		return events;
	}

	/**
	 * @param events the events to set
	 */
	public void setEvents(Event[] events) {
		this.events = events;
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
    
    
    

}
