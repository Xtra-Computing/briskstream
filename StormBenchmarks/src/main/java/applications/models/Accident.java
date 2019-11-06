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

package applications.models;

import applications.datatypes.PositionReport;
import applications.datatypes.util.Constants;
import applications.datatypes.util.SegmentIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Time;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;


/**
 * The {@code Accident} class serves to record both current and historical data about accidents.
 *
 * @author richter
 */
public class Accident implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(Accident.class);
    private long startMinute;
    private int position;
    private boolean over = false;
    private Set<SegmentIdentifier> involvedSegs = new HashSet<>();
    private Set<Integer> involvedCars = new HashSet<>();
//	private int maxPos;
//	private int minPos;
    /**
     * timestamp of last positionreport indicating that the accident is still active
     */
    private long lastUpdateTime = Integer.MAX_VALUE - 1;
    private PositionReport posReport;

    protected Accident() {
    }

    // private int maxPos = -1;
    // private int minPos = -1;


    public Accident(long startMinute, long lastUpdateTime, int position, int maxPos, int minPos,
                    PositionReport posReport) {
        this.startMinute = startMinute;
        this.lastUpdateTime = lastUpdateTime;
        this.position = position;
//		this.maxPos = maxPos;
//		this.minPos = minPos;
        this.posReport = posReport;
    }

    public Accident(Accident accident) {
        this.position = accident.getAccidentPosition();
        this.lastUpdateTime = accident.getLastUpdateTime();
        this.involvedSegs = accident.getInvolvedSegs();
        this.involvedCars = accident.getInvolvedCars();
        this.over = accident.isOver();
        this.posReport = accident.getPosReport();
        this.startMinute = Time.getMinute(this.getPosReport().getTime());
    }

    public Accident(PositionReport report) {
        this.position = report.getPosition();
        this.lastUpdateTime = report.getTime();
        this.assignSegments(report.getXWay(), report.getPosition(), report.getDirection());
        this.posReport = report;
    }

    public static void validatePositionReport(PositionReport pos) {
        // if(pos.getProcessingTimeSec() > 5) {
        // throw new IllegalArgumentException("Time Requirement not met: " + pos.getProcessingTimeSec() + " for "
        // + pos);
        // }

    }

    /**
     * assigns segments to accidents according to LRB req. (within 5 segments upstream)
     *
     * @param xway of accident
     * @param pos  of accident
     * @param dir  of accident
     */
    private void assignSegments(int xway, int pos, short dir) {

        short segment = (short) (pos / Constants.NUMBER_OF_POSITIONS);

        if (dir == 0) {
            // maxPos = pos; minPos = (segment-4)*5280;
            for (short i = segment; 0 < i && i > segment - 5; i--) {
                SegmentIdentifier segmentTriple = null;// new SegmentIdentifier(xway, i, dir);
                this.involvedSegs.add(segmentTriple);
            }
        } else {
            // minPos = pos; maxPos = (segment+5)*5280-1;
            for (short i = segment; i < segment + 5 && i < 100; i++) {
                SegmentIdentifier segmentTriple = null;// new SegmentIdentifier(xway, i, dir);
                this.involvedSegs.add(segmentTriple);
            }
        }

        //LOG.DEBUG("ACC:: assigned segments to accident: " + this.involvedSegs.toString());
    }

    public boolean active(long minute) {
        if (this.isOver()) {
            return minute <= Time.getMinute(this.lastUpdateTime);
        } else {
            return minute > this.startMinute;
        }
    }

    /**
     * get accident position
     *
     * @return position number
     */
    public int getAccidentPosition() {
        return this.position;
    }

    public PositionReport getPosReport() {
        return this.posReport;
    }

    public void setPosReport(PositionReport posReport) {
        this.posReport = posReport;
    }

    public boolean isOver() {
        return this.over;
    }

    public void setOver(long timeinseconds) {
        this.over = true;
    }

    public void setOver(boolean over) {
        this.over = over;
    }

    /**
     * a shortcut for retrieval of {@code posReport.time}
     *
     * @return
     */
    public long getStartTime() {
        return this.getPosReport().getTime();
    }

    public long getLastUpdateTime() {
        return this.lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    /**
     * get all vids of vehicles involved in that accident
     *
     * @return vehicles hashset wtih vids
     */
    public Set<Integer> getInvolvedCars() {
        return this.involvedCars;
    }

    public Set<SegmentIdentifier> getInvolvedSegs() {
        return this.involvedSegs;
    }

    protected void recordUpdateTime(int curTime) {
        this.lastUpdateTime = curTime;
    }

    public boolean active(int minute) {
        return (minute > this.getStartTime() / 60 && minute <= (Time.getMinute(this.lastUpdateTime)));
    }

    /**
     * Checks if accident
     *
     * @param minute
     * @return
     */
    public boolean over(int minute) {
        return (minute > (this.lastUpdateTime + 1));
    }

    /**
     * update accident information (includes updating time and adding vid of current position report if not already
     * present
     *
     * @param report positionreport of accident car
     */
    public void updateAccident(PositionReport report) {
        // add car id to involved cars if not there yet
        this.getInvolvedCars().add(report.getVid());
        this.lastUpdateTime = report.getTime();

    }

    /**
     * add car ids of involved cars to accident info
     *
     * @param vehicles hashset wtih vids
     */
    public void addAccVehicles(HashSet<Integer> vehicles) {
        this.getInvolvedCars().addAll(vehicles);
    }

}
