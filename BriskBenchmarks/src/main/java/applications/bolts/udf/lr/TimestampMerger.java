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

package applications.bolts.udf.lr;

import applications.datatype.util.TimeStampExtractor;
import brisk.components.TopologyComponent;
import brisk.components.context.TopologyContext;
import brisk.components.grouping.Grouping;
import brisk.components.operators.api.AbstractBolt;
import brisk.components.operators.base.MapBolt;
import brisk.execution.ExecutionNode;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;


/**
 * {@link TimestampMerger} merges all incoming streams (all physical substreams from all tasks) over all logical
 * producers in ascending timestamp order. Input tuples must be in ascending timestamp order within each incoming
 * substream. The timestamp attribute is expected to be of type {@link Number}.
 *
 * @author Matthias J. Sax
 */
public class TimestampMerger extends MapBolt {
    private final static long serialVersionUID = -6930627449574381467L;
    private final static Logger LOG = LoggerFactory.getLogger(TimestampMerger.class);

    /**
     * The original bolt that consumers a stream of input tuples that are ordered by their timestamp attribute.
     */
    private final AbstractBolt wrappedBolt;

    /**
     * The index of the timestamp attribute ({@code -1} if attribute name or timestamp extractor is used).
     */
    private final int tsIndex;

    /**
     * The name of the timestamp attribute ({@code null} if attribute index or timestamp extractor is used).
     */
    private final String tsAttributeName;

    /**
     * The extractor for the timestamp ({@code null} if attribute index or name is used).
     */
    private final TimeStampExtractor<Tuple> tsExtractor;

    /**
     * Input tuple buffer for merging.
     */
    private StreamMerger<Tuple> merger;


    /**
     * Instantiates a new {@link TimestampMerger} that wrapped the given bolt.
     *
     * @param wrappedBolt The bolt to be wrapped.
     * @param tsIndex     The index of the timestamp attribute.
     */
    public TimestampMerger(AbstractBolt wrappedBolt, int tsIndex) {
        super(LOG, new HashMap<>());
        assert (wrappedBolt != null);
        assert (tsIndex >= 0);

        //LOG.DEBUG("Initialize with timestamp index {}", new Integer(tsIndex));

        this.wrappedBolt = wrappedBolt;
        this.tsIndex = tsIndex;
        this.tsAttributeName = null;
        this.tsExtractor = null;
    }

    /**
     * Instantiates a new {@link TimestampMerger} that wrapped the given bolt.
     *
     * @param wrappedBolt     The bolt to be wrapped.
     * @param tsAttributeName The name of the timestamp attribute.
     */
    public TimestampMerger(AbstractBolt wrappedBolt, String tsAttributeName) {
        super(LOG, new HashMap<>());
        assert (wrappedBolt != null);
        assert (tsAttributeName != null);

        //LOG.DEBUG("Initialize with timestamp attribute {}", tsAttributeName);

        this.wrappedBolt = wrappedBolt;
        this.tsIndex = -1;
        this.tsAttributeName = tsAttributeName;
        this.tsExtractor = null;
    }

    /**
     * Instantiates a new {@link TimestampMerger} that wrapped the given bolt.
     *
     * @param wrappedBolt The bolt to be wrapped.
     * @param tsExtractor The extractor for the timestamp.
     */
    public TimestampMerger(AbstractBolt wrappedBolt, TimeStampExtractor<Tuple> tsExtractor) {
        super(LOG, new HashMap<>());
        assert (wrappedBolt != null);
        assert (tsExtractor != null);

        //LOG.DEBUG("Initialize with timestamp extractor");

        this.wrappedBolt = wrappedBolt;
        this.tsIndex = -1;
        this.tsAttributeName = null;
        this.tsExtractor = tsExtractor;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        // for each logical input stream (ie, each producer bolt), we get an input partition for each of its tasks
        LinkedList<Integer> taskIds = new LinkedList<Integer>();
//		for (Entry<GlobalStreamId, Grouping> inputStream : arg1.getThisSources().entrySet()) {
//			GlobalStreamId key = inputStream.getKey();
//			taskIds.addAll(arg1.getComponentTasks(key.get_componentId()));
//		}
        HashMap<String, Map<TopologyComponent, Grouping>> parents = context.getThisComponent().getParents();

        for (Map<TopologyComponent, Grouping> map : parents.values()) {
            for (TopologyComponent topologyComponent : map.keySet()) {
                for (ExecutionNode producer : topologyComponent.getExecutorList()) {
                    taskIds.add(producer.getExecutorID());
                }
            }

        }

        //LOG.DEBUG("Detected producer tasks: {}", taskIds);

        if (this.tsIndex != -1) {
            assert (this.tsAttributeName == null && this.tsExtractor == null);
            this.merger = new StreamMerger<>(taskIds, this.tsIndex);
        } else if (this.tsAttributeName != null) {
            assert (this.tsExtractor == null);
            this.merger = new StreamMerger<>(taskIds, this.tsAttributeName);
        } else {
            assert (this.tsExtractor != null);
            this.merger = new StreamMerger<>(taskIds, this.tsExtractor);
        }

//		this.wrappedBolt.prepare(conf, context, outputCollector);
    }

    @Override
    public void execute(Tuple tuple) throws InterruptedException {
//		LOG.trace("Adding tuple to internal buffer tuple: {}", tuple);
//		this.merger.addTuple(new Integer(tuple.getSourceTask()), tuple);
//
//		Tuple t;
//		while ((t = this.merger.getNextTuple()) != null) {
//			LOG.trace("Extrated tuple from internal buffer for processing: {}", tuple);
//			this.wrappedBolt._execute(t);
//		}

        LOG.trace("Adding tuple to internal buffer tuple: {}", tuple);
        this.merger.addTuple(tuple.getSourceTask(), tuple);

        Tuple t;
        while ((t = this.merger.getNextTuple()) != null) {
            LOG.trace("Extrated tuple from internal buffer for processing: {}", tuple);
            this.wrappedBolt._execute(t);
        }
    }


    @Override
    public void cleanup() {
        this.wrappedBolt.cleanup();
    }

}
