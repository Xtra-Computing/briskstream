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
package applications.datatypes;


import applications.util.Time;
import org.apache.storm.tuple.Values;


/**
 * Base class for all LRB tuples.<br />
 * <br />
 * All tuples do have the following two attributes: TYPE, TIME
 * <ul>
 * <li>TYPE: the tuple type ID</li>
 * <li>TIME: 'the timestamp of the input tuple that triggered the tuple to be generated' (in seconds)</li>
 * <ul>
 *
 * @author mjsax
 */
public abstract class AbstractLRBTuple extends Values {
    /**
     * The tuple type ID for position reports.
     */
    public final static short position_report = 0;

    // LRB input types
    /**
     * The tuple type ID for position reports as object (see {@link #position_report}).
     */
    @SuppressWarnings("boxing")
    public final static Short POSITION_REPORT = position_report;
    /**
     * The tuple type ID for account balance requests.
     */
    public final static short account_balance_request = 2;
    /**
     * The tuple type ID for account balance requests as object (see {@link #account_balance_request}).
     */
    @SuppressWarnings("boxing")
    public final static Short ACCOUNT_BALANCE_REQUEST = account_balance_request;
    /**
     * The tuple type ID for daily expenditure requests.
     */
    public final static short daily_expenditure_request = 3;
    /**
     * The tuple type ID for daily expenditure requests as object (see {@link #daily_expenditure_request}).
     */
    @SuppressWarnings("boxing")
    public final static Short DAILY_EXPENDITURE_REQUEST = daily_expenditure_request;
    /**
     * The tuple type ID for travel time requests.
     */
    public final static short travel_time_request = 4;
    /**
     * The tuple type ID for travel time requests as object (see {@link #travel_time_request}).
     */
    @SuppressWarnings("boxing")
    public final static Short TRAVEL_TIME_REQUEST = travel_time_request;
    /**
     * The tuple type ID for toll notifications.
     */
    public final static short toll_notification = 0;

    // LRB output types
    /**
     * The tuple type ID for toll notifications as object (see {@link #toll_notification}).
     */
    @SuppressWarnings("boxing")
    public final static Short TOLL_NOTIFICATION = toll_notification;
    /**
     * The tuple type ID for accident notifications.
     */
    public final static short accident_notification = 1;
    /**
     * The tuple type ID for accident notifications as object (see {@link #accident_notification}).
     */
    @SuppressWarnings("boxing")
    public final static Short ACCIDENT_NOTIFICATION = accident_notification;
    /**
     * The index of the TYPE attribute.
     */
    public final static int TYPE_IDX = 0;

    // attribute indexes
    /**
     * The index of the TIME attribute.
     */
    public final static int TIME_IDX = 1;
    private final static long serialVersionUID = -1117500573019912901L;


    protected AbstractLRBTuple() {
    }

    protected AbstractLRBTuple(Short type, Integer time) {
        assert (type != null);
        assert (time != null);
        assert (type.shortValue() == position_report || type.shortValue() == account_balance_request
                || type.shortValue() == daily_expenditure_request || type.shortValue() == travel_time_request
                || type.shortValue() == toll_notification || type.shortValue() == accident_notification);
        assert (time.shortValue() >= 0);

        super.add(TYPE_IDX, type);
        super.add(TIME_IDX, time);

        assert (super.size() == 2);
    }


    /**
     * Returns the tuple type ID of this {@link AbstractLRBTuple}.
     *
     * @return the type ID of this tuple
     */
    public final Short getType() {
        return (Short) super.get(TYPE_IDX);
    }

    /**
     * Returns the timestamp (in LRB seconds) of this {@link AbstractLRBTuple}.
     *
     * @return the timestamp of this tuple
     */
    public final Integer getTime() {
        return (Integer) super.get(TIME_IDX);
    }

    /**
     * TODO remove class Time ???
     *
     * @return
     */
    public final short getMinuteNumber() {
        return Time.getMinute(this.getTime().shortValue());
    }

}
