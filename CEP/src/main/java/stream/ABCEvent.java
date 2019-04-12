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
package stream;


/**
 * This class represents a kind of event.
 * @author haopeng
 *
 */
public class ABCEvent implements Event{
	/**
	 * Event id
	 */
	int id;
	
	/**
	 * Event timestamp
	 */
	int timestamp;
	
	/**
	 * event type
	 */
	String eventType;
	
	/**
	 * Price, an attribute
	 */
	int price;
	
	/**
	 * Constructor
	 */
	public  ABCEvent(int i, int ts, String et, int p){
		id = i;
		timestamp = ts;
		eventType = et;
		price = p;
		
	}
	
	/**
	 * @return the cloned event
	 */
	public Object clone(){
		ABCEvent o = null;
		try {
			o = (ABCEvent)super.clone();
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return o;
	}

	/**
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * @return the timestamp
	 */
	public int getTimestamp() {
		return timestamp;
	}

	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
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
	 * @return the price
	 */
	public int getPrice() {
		return price;
	}

	/**
	 * @param price the price to set
	 */
	public void setPrice(int price) {
		this.price = price;
	}

	public String toString(){
		return "ID="+ id + "\tTimestamp=" + timestamp
			+ "\tEventType=" + eventType + "\tPrice =" + price;
	}

	/* (non-Javadoc)
	 * @see edu.umass.cs.sase.mvc.model.Event#getAttributeByName(java.lang.String)
	 */
	public int getAttributeByName(String attributeName) {
		
		if(attributeName.equalsIgnoreCase("price"))
			return price;
		if(attributeName.equalsIgnoreCase("id"))
			return this.id;
		if(attributeName.equalsIgnoreCase("timestamp"))
			return this.timestamp;
		
		return -1;
	}

	/* (non-Javadoc)
	 * @see edu.umass.cs.sase.mvc.model.Event#getAttributeByNameString(java.lang.String)
	 */
	public String getAttributeByNameString(String attributeName) {
		// TODO Auto-generated method stub
		return null;
	}
	/* (non-Javadoc)
	 * @see edu.umass.cs.sase.mvc.model.Event#getAttributeValueType(java.lang.String)
	 */
	public int getAttributeValueType(String attributeName) {
		// TODO Auto-generated method stub
		return 0;
	}

	/* (non-Javadoc)
	 * @see edu.umass.cs.sase.mvc.model.Event#getAttributeByNameDouble(java.lang.String)
	 */
	public double getAttributeByNameDouble(String attributeName) {
		// TODO Auto-generated method stub
		return 0;
	}







	
	

}
