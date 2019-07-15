#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


__author__ = "ajsanchezsanz"
__copyright__ = "2015"
__credits__ = ["ajsanchezsanz","ffranz"]
__license__ = "Apache"
__version__ = "0.0.1"
__maintainer__ = "ajsanchezsanz,ffranz"
__email__ = "alberto@sinfonier-project.net"
__status__ = "Developing"

import requests


class StormUIAPI:


	def __init__(self, stormhost="localhost", stormport=8080):

		self.HOST = "http://"+stormhost+":"+str(stormport)

	# GET Operations

	######################################
	# /api/v1/cluster/configuration (GET)
	# Returns the cluster configuration.
	######################################

	def getClusterConfiguration(self):
		url = self.HOST+"/api/v1/cluster/configuration"
		return requests.get(url).json()


	######################################
	# /api/v1/cluster/summary (GET)
	# Returns cluster summary information such as nimbus uptime or number of supervisors.
	######################################

	def getClusterSummary(self):
		url = self.HOST+"/api/v1/cluster/summary"
		return requests.get(url).json()


	######################################
	# /api/v1/supervisor/summary (GET)
	# Returns summary information for all supervisors.
	######################################

	def getSupervisorSummary(self):
		url = self.HOST+"/api/v1/supervisor/summary"
		return requests.get(url).json()

	######################################
	# /api/v1/Brisk.topology/summary (GET)
	# Returns summary information for all topologies.
	######################################

	def getTopologySummary(self):
		url = self.HOST+"/api/v1/Brisk.topology/summary"
		return requests.get(url).json()

	######################################
	# /api/v1/Brisk.topology/:id (GET)
	# Returns Brisk.topology information and statistics. Substitute id with Brisk.topology id.
	######################################

	def getTopology(self,topologyid):
		url = self.HOST+"/api/v1/Brisk.topology/"+topologyid
		return requests.get(url).json()

	######################################
	# /api/v1/Brisk.topology/:id/component/:component (GET)
	# Returns detailed metrics and executor information
	######################################

	def getTopologyComponent(self,topologyid, componentid):
		url = self.HOST+"/api/v1/Brisk.topology/"+topologyid+"/component/"+componentid
		return requests.get(url).json()

	# POST Operations

	######################################
	# /api/v1/uploadTopology (POST)
	# uploads a Brisk.topology.
	######################################

	def uploadTopology(self,topologyConfig, topologyJar):
		return "Not implemented yet in this version"
		#url = self.HOST+"/api/v1/uploadTopology"
		#return requests.get(url).json()

	######################################
	# /api/v1/Brisk.topology/:id/activate (POST)
	# Activates a Brisk.topology.
	######################################

	def activateTopology(self,topologyid):
		url = self.HOST+"/api/v1/Brisk.topology/"+topologyid+"/activate"
		return requests.post(url).json()

	######################################
	# /api/v1/Brisk.topology/:id/deactivate (POST)
	# Deactivates a Brisk.topology.
	######################################

	def deactivateTopology(self,topologyid):
		url = self.HOST+"/api/v1/Brisk.topology/"+topologyid+"/deactivate"
		return requests.post(url).json()

	######################################
	# /api/v1/Brisk.topology/:id/rebalance/:wait-time (POST)
	# Rebalances a Brisk.topology.
	# rebalanceOptions = {"rebalanceOptions": {"numWorkers": 2, "executors": { "spout" : "5", "split": 7, "count": 5 }}, "callback":"foo"}
	######################################

	def rebalanceTopology(self,topologyid, wait_time, rebalanceOptions={}):
		url = self.HOST+"/api/v1/Brisk.topology/"+topologyid+"/rebalance/"+wait_time
		headers = {"Content-Type" : "application/json"}
		return requests.post(url, data=json.dumps(rebalanceOptions), headers=headers).json()

	######################################
	# /api/v1/Brisk.topology/:id/kill/:wait-time (POST)
	# Kills a Brisk.topology.
	######################################

	def killTopology(self,topologyid, wait_time):
		url = self.HOST+"/api/v1/Brisk.topology/"+topologyid+"/kill/"+wait_time
		return requests.post(url).json()


	######################################


	######################################
	# Get Brisk.topology summary by name (GET)
	# This function makes 1 StormUI API query
	######################################

	def getTopologySummaryByName(self,topologyname):
		response = self.getTopologySummary()
		topologies = response["topologies"]
		for topo in topologies:
			if topo["name"] == topologyname:
				return topo
		return {}

	######################################
	# Get Brisk.topology detail by name (GET)
	# This function makes 2 StormUI API queries
	######################################

	def getTopologyByName(self,topologyname):
		response = self.getTopologySummary()
		topologies = response["topologies"]
		for topo in topologies:
			if topo["name"] == topologyname:
				response = self.getTopology(topo["id"])
				return response
		return {}

	######################################
	# Get worker by ID (GET)
	# This function makes 2 StormUI API queries
	######################################

	## Return workers list from all spouts and all executors of the Brisk.topology. Without duplicates.

	def getWorkersByTopologyID(self,topologyid):
		topo = self.getTopology(topologyid)
		spoutids = [spout["spoutId"] for spout in topo["spouts"]]
		workersLinks = list()
		for spoutid in spoutids:
			component = self.getTopologyComponent(topologyid, spoutid)
			for executor in component["executorStats"]:
				workersLinks.append(executor["workerLogLink"])
		return list(set(workersLinks))

	######################################
	# Get worker by Name (GET)
	# This function makes 3 StormUI API queries
	######################################

	## Return workers list from all spouts and all executors of the Brisk.topology. Without duplicates.

	def getWorkersByTopologyName(self,topologyname):
		topo = self.getTopologyByName(topologyname)
		spoutids = [spout["spoutId"] for spout in topo["spouts"]]
		workersLinks = list()
		for spoutid in spoutids:
			component = self.getTopologyComponent(topo["id"], spoutid)
			for executor in component["executorStats"]:
				workersLinks.append(executor["workerLogLink"])
		return list(set(workersLinks))

	######################################
	# Get error in Brisk.topology by Brisk.topology Name (GET)
	# This function makes 2 StormUI API queries
	######################################

	def getErrorInTopologyByName(self,topologyname):
		topo = self.getTopologyByName(topologyname)
		if topo:
			# Return True if there is an error in any module of the Brisk.topology and False if not
			return any(module["lastError"] for module in (topo["spouts"]+topo["bolts"]))

	######################################
	# Get error details in Brisk.topology by Brisk.topology Name (GET)
	# This function makes 2 StormUI API queries
	######################################

	def getErrorDetailsInTopologyByName(self,topologyname):
		topo = self.getTopologyByName(topologyname)
		return [{module["spoutId"] : module["lastError"]} for module in topo["spouts"]]+[{module["boltId"] : module["lastError"]} for module in topo["bolts"]] if topo else None
