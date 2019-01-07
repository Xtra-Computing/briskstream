/*
 * #!
 * %
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #_
 */

package applications.models.lr;

import applications.bolts.lr.data.AbstractLRBTuple;
import applications.bolts.lr.data.AbstractOutputTuple;
import applications.bolts.lr.data.AccountBalanceRequest;
import applications.bolts.lr.data.util.TopologyControl;
import org.apache.storm.tuple.Fields;

import static applications.constants.BaseConstants.BaseField.MSG_ID;
import static applications.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;

/**
 * The reply to a {@link AccountBalanceRequest}.
 *
 * @author richter
 */
public class AccountBalance extends AbstractOutputTuple {
	private static final long serialVersionUID = 1L;
	private int queryIdentifier;
	private int balance;
	private long tollTime;

	public AccountBalance(Integer time, int queryIdentifier, int balance, long tollTime, Integer created) {
		super(AbstractLRBTuple.ACCOUNT_BALANCE_REQUEST, time, created);
		this.queryIdentifier = queryIdentifier;
		this.balance = balance;
		this.tollTime = tollTime;
	}

	public static Fields getSchema() {
		return new Fields(
				TopologyControl.QUERY_ID_FIELD_NAME,
				TopologyControl.BALANCE_NOTIFICATION_REQUESTS_FIELD_NAME,
				TopologyControl.TOLL_ASSESSMENTS_FILE_WRITER_BOLT_NAME);

	}

	public static Fields getLatencySchema() {
		return new Fields(
				TopologyControl.QUERY_ID_FIELD_NAME,
				TopologyControl.BALANCE_NOTIFICATION_REQUESTS_FIELD_NAME,
				TopologyControl.TOLL_ASSESSMENTS_FILE_WRITER_BOLT_NAME, MSG_ID, SYSTEMTIMESTAMP

		);

	}

	public long getTollTime() {
		return this.tollTime;
	}

	public void setTollTime(long tollTime) {
		this.tollTime = tollTime;
	}

	public int getQueryIdentifier() {
		return this.queryIdentifier;
	}

	protected void setQueryIdentifier(int queryIdentifier) {
		this.queryIdentifier = queryIdentifier;
	}

	public int getBalance() {
		return this.balance;
	}

	protected void setBalance(int balance) {
		this.balance = balance;
	}
}
