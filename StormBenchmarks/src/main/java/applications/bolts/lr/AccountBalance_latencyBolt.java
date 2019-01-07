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

package applications.bolts.lr;


import applications.bolts.AbstractBolt;
import applications.constants.BaseConstants;
import applications.bolts.lr.data.AccountBalanceRequest;
import applications.bolts.lr.data.PositionReport;
import applications.bolts.lr.data.util.TopologyControl;
import applications.models.lr.AccountBalance;
import applications.models.lr.VehicleAccount;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static applications.constants.BaseConstants.BaseField.MSG_ID;


/**
 * This bolt recieves the toll values assesed by the {@link TollNotificationBolt} and answers to account balance
 * queries. Therefore it processes the streams {@link TopologyControl#TOLL_ASSESSMENTS_STREAM_ID} (for the former) and
 * {@link TopologyControl#ACCOUNT_BALANCE_REQUESTS_STREAM_ID} (for the latter)
 */
public class AccountBalance_latencyBolt extends AbstractBolt {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(AccountBalance_latencyBolt.class);
	private final PositionReport inputPositionReport = new PositionReport();
	double cnt = 0, cnt1 = 0, cnt2 = 0;
	/**
	 * Contains all vehicles and the accountinformation of the current day.
	 */
	private Map<Integer, VehicleAccount> allVehicles;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		super.prepare(conf, context, collector);
		this.allVehicles = new HashMap<>();
	}

	@Override
	public synchronized void execute(Tuple tuple) {
		Long msgId;
		Long SYSStamp;

		msgId = tuple.getLongByField(MSG_ID);
		SYSStamp = tuple.getLongByField(BaseConstants.BaseField.SYSTEMTIMESTAMP);

		if (tuple.getSourceStreamId().equals(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID)) {
			this.getBalanceAndSend(tuple, msgId, SYSStamp);

		} else if (tuple.getSourceStreamId().equals(TopologyControl.TOLL_ASSESSMENTS_STREAM_ID)) {
			this.updateBalance(tuple);

		} else {
			throw new RuntimeException(String.format("Erroneous stream subscription. Please report a bug at %s",
					"tonyzhang19900609@gmail.com"));
		}
	}

	private synchronized void updateBalance(Tuple tuple) {
		Integer vid = tuple.getIntegerByField(TopologyControl.VEHICLE_ID_FIELD_NAME);
		VehicleAccount account = this.allVehicles.get(vid);
		PositionReport pos = (PositionReport) tuple.getValueByField(TopologyControl.POS_REPORT_FIELD_NAME);

		if (account == null) {
			int assessedToll = 0;
			//TODO:assume it's 0 now.
			account = new VehicleAccount(assessedToll, pos.getVid(), pos.getXWay(), Long.valueOf(pos.getTime()));
			this.allVehicles.put(tuple.getIntegerByField(TopologyControl.VEHICLE_ID_FIELD_NAME), account);
		} else {
			account.updateToll(tuple.getIntegerByField(TopologyControl.TOLL_FIELD_NAME));
		}
	}

	private synchronized void getBalanceAndSend(Tuple tuple, Long msgId, Long SYSStamp) {
		AccountBalanceRequest bal = new AccountBalanceRequest(
				tuple.getIntegerByField(TopologyControl.TIME_FIELD_NAME),
				tuple.getIntegerByField(TopologyControl.VEHICLE_ID_FIELD_NAME),
				tuple.getIntegerByField(TopologyControl.QUERY_ID_FIELD_NAME));

		VehicleAccount account = this.allVehicles.get(bal.getVid());

		if (account == null) {
			//LOG.DEBUG("No account information available yet: at:" + bal.getTime() + " for request" + bal);
			AccountBalance accountBalance = new AccountBalance
					(
							bal.getTime(),
							bal.getQid(),
							0, // balance
							0, // tollTime
							bal.getTime()
					);
			accountBalance.add(msgId);
			accountBalance.add(SYSStamp);
			this.collector.emit(TopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, accountBalance);
		} else {
			AccountBalance accountBalance
					= account.getAccBalanceNotification(bal);
			accountBalance.add(msgId);
			accountBalance.add(SYSStamp);
			this.collector.emit(TopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, accountBalance);
		}
	}

	public Map<Integer, VehicleAccount> getAllVehicles() {
		return this.allVehicles;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(
				TopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, AccountBalance.getLatencySchema()
		);
	}

}
