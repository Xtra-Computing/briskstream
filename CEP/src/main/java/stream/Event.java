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
 * This is the interface for events that would be processed by the engine.
 * @author haopeng
 * 
 */
public interface Event {
	
	/**
	 * Returns the value of the attribute with the required name
	 * @param attributeName the required attribute name
	 * @return the value of the attribute, integer
	 */
	public int getAttributeByName(String attributeName);
	
	/**
	 * Returns the value of the attribute with the required name
	 * @param attributeName the required attribute name
	 * @return the value of the attribute, double
	 */
	public double getAttributeByNameDouble(String attributeName);
	
	/**
	 * Returns the value of the attribute with the required name
	 * @param attributeName the required attribute name
	 * @return the value of the attribute, string
	 */
	public String getAttributeByNameString(String attributeName);
	
	/**
	 * Returns the value type of the attribute
	 * @param attributeName the required attribute name
	 * @return the code representing the type, 0 for integer, 1 for double, 2 for string
	 */
	public int getAttributeValueType(String attributeName);
	
	/**
	 * 
	 * @return the event id
	 */
	public int getId();
	
	/**
	 * 
	 * @param Id the event id to set
	 */
	public void setId(int Id);
	
	/**
	 * 
	 * @return self description
	 */
	public String toString();
	
	/**
	 * 
	 * @return the timestamp of the event
	 */
	public int getTimestamp();

	/**
	 * 
	 * @return the event type
	 */
	public String getEventType();
	
	/**
	 * 
	 * @return the cloned event
	 */
	public Object clone();




}
