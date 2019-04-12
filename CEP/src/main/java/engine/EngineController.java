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

import net.sourceforge.jeval.EvaluationException;
import query.NFA;
import stream.Stream;

/**
 * This class is used to wrap the Engine class, 
 * such that when you write code, you can quickly locate the related methods.
 * @author haopeng
 *
 */
public class EngineController {
	/**
	 * The engine
	 */
	Engine myEngine;
	/**
	 * Initializes the engine
	 */
	public void initializeEngine(){
		myEngine.initialize();
	}
	/**
	 * Default constructor.
	 */
	public EngineController(){
		myEngine = new Engine();		
	}
	/**
	 * Constructor, can set different kinds of engines by different parameters
	 * @param engineType specifies the engine type, currently supports "sharingengine"
	 */
	public EngineController(String engineType){
		
			myEngine = new Engine();
		
	}
/**
 * Sets the nfa and selection strategy for the engine
 * @param selectionStrategy the selection strategy
 * @param nfaLocation the nfa file for the query
 */
	public void setNfa(String selectionStrategy, String nfaLocation){
		NFA nfa = new NFA(selectionStrategy, nfaLocation);
		myEngine.setNfa(nfa);
	}
/**
 * Sets the nfa for the engine.	
 * @param nfaLocation the nfa file for the query
 */
	public void setNfa(String nfaLocation){
		NFA nfa = new NFA(nfaLocation);
		myEngine.setNfa(nfa);
	}
/**
 * Sets the input stream for the engine
 * @param input the input stream
 */
	public void setInput(Stream input){
		myEngine.setInput(input);
	}
/**
 * starts to run the engine	
 * @throws CloneNotSupportedException
 * @throws EvaluationException
 */
	public void runEngine() throws CloneNotSupportedException, EvaluationException{
		myEngine.runEngine();
		
		/*
			if(myEngine.getNfa().getSelectionStrategy().equalsIgnoreCase("partition-contiguity")){
				myEngine.runPartitionContiguityEngine();
			}else{
			myEngine.runEngine();
			
			}
		*/
		
	}

	

	
}
