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
 * This class represents the stock event.
 * @author haopeng
 *
 */
public class StockEvent implements Event{
	/**
	 * Event id
	 */
	int id;
	
	/**
	 * Event timestamp
	 */
	int timestamp;
	
	/**
	 * Event type
	 */
	String eventType;
	
	/**
	 * Symbol, an attribute
	 */
	int symbol;
	
	/**
	 * Price, an attribute
	 */
	int price;
	
	/**
	 * Volume, an attribute
	 */
	int volume;
	
	
	/**
	 * Constructor
	 */
	public StockEvent(int id, int ts, int symbol, int price, int volume){
		this.id = id;
		this.timestamp = ts;
		this.symbol = symbol;
		this.price = price;
		this.volume = volume;
		this.eventType = "stock";
	}
	/**
	 * Another constructor
	 * @param id
	 * @param ts
	 * @param symbol
	 * @param price
	 * @param volume
	 * @param type
	 */
	public StockEvent(int id, int ts, int symbol, int price, int volume, String type){
		this.id = id;
		this.timestamp = ts;
		this.symbol = symbol;
		this.price = price;
		this.volume = volume;
		this.eventType = type;
	}
	/**
	 * Returns the attribute value for the given attribute
	 * @param attributeName The name of the attribute to be returned
	 */
	public int getAttributeByName(String attributeName) {
		if(attributeName.equalsIgnoreCase("symbol"))
			return this.symbol;
		if(attributeName.equalsIgnoreCase("price"))
			return price;
		if(attributeName.equalsIgnoreCase("volume"))
			return this.volume;
		if(attributeName.equalsIgnoreCase("id"))
			return this.id;
		if(attributeName.equalsIgnoreCase("timestamp"))
			return this.timestamp;
		
		return 0;
		
	}

	
	public String getEventType() {
		// TODO Auto-generated method stub
		return this.eventType;
	}


	public int getId() {
		// TODO Auto-generated method stub
		return this.id;
	}


	public int getTimestamp() {
		// TODO Auto-generated method stub
		return this.timestamp;
		
	}
	
	public String toString() {
		return "ID = "+ id + "\tTimestamp = " + timestamp
		+ "\tSymbol = " + this.symbol + "\tPrice = " + price + "\tVolume = " + volume;
	}

	/**
	 * @return the symbol
	 */
	public int getSymbol() {
		return symbol;
	}

	/**
	 * @param symbol the symbol to set
	 */
	public void setSymbol(int symbol) {
		this.symbol = symbol;
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

	/**
	 * @return the volume
	 */
	public int getVolume() {
		return volume;
	}

	/**
	 * @param volume the volume to set
	 */
	public void setVolume(int volume) {
		this.volume = volume;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * @param eventType the eventType to set
	 */
	public void setEventType(String eventType) {
		this.eventType = eventType;
	}



	

	/**
	 * Clones the event
	 */
	public Object clone(){
		StockEvent o = null;
		try {
			o = (StockEvent)super.clone();
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return o;
	}

	/* (non-Javadoc)
	 * @see edu.umass.cs.sase.mvc.model.Event#getAttributeByNameDouble(java.lang.String)
	 */
	public double getAttributeByNameDouble(String attributeName) {
		// TODO Auto-generated method stub
		return 0;
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


	


	

}
