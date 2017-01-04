import os
import sys
import gzip
import logging
import datetime
import sqlite3
import uuid
from time import time
from multiprocessing import Queue, Process
import getpass
import socket
import struct

import requests
import ujson as json
from elasticsearch import Elasticsearch
from elasticsearch.client import ClusterClient, IndicesClient
from elasticsearch.helpers import parallel_bulk

sys.path.insert(1, sys.path[0] + "/lib")
from ip2asn import IP2ASN

RFC1918_10_START = struct.unpack("!I", socket.inet_aton("10.0.0.0"))
RFC1918_10_END = struct.unpack("!I", socket.inet_aton("10.255.255.255"))
RFC1918_192_START = struct.unpack("!I", socket.inet_aton("192.168.0.0"))
RFC1918_192_END = struct.unpack("!I", socket.inet_aton("192.168.255.255"))
RFC1918_172_START = struct.unpack("!I", socket.inet_aton("172.16.0.0"))
RFC1918_172_END = struct.unpack("!I", socket.inet_aton("172.31.255.255"))
RFC1918_169_START = struct.unpack("!I", socket.inet_aton("169.254.0.0"))
RFC1918_169_END = struct.unpack("!I", socket.inet_aton("169.254.255.255"))
RFC1918_127_START = struct.unpack("!I", socket.inet_aton("127.0.0.0"))
RFC1918_127_END = struct.unpack("!I", socket.inet_aton("127.255.255.255"))

def rfc1918(ipint):
	if (ipint >= RFC1918_10_START and ipint <= RFC1918_10_END) or\
		(ipint >= RFC1918_192_START and ipint <= RFC1918_192_END) or\
		(ipint >= RFC1918_172_START and ipint <= RFC1918_172_END) or\
		(ipint >= RFC1918_169_START and ipint <= RFC1918_169_END) or\
		(ipint >= RFC1918_127_START and ipint <= RFC1918_127_END):
		return True

class Indexer:
	INDEX_PREFIX = "pulsar"
	def __init__(self, conf, queue):
		self.conf = conf
		host = self.conf.get("host", "es")
		port = self.conf.get("port", 9200)
		self.log = logging.getLogger("pulsar.indexer")
		logging.getLogger("elasticsearch").setLevel(logging.INFO)
		self.log.debug("port: %r" % port)
		self.es = Elasticsearch([{"host": host, "port": port}])
		self.cluster_client = ClusterClient(self.es)
		health = self.cluster_client.health()
		if not health or health.get("number_of_nodes") < 1:
			raise Exception("No Elasticsearch nodes found: %r" % health)
		# Put our template
		self.indices_client = IndicesClient(self.es)
		self.index_prefix = self.conf.get("index_prefix", self.INDEX_PREFIX)
		self.indices_client.put_template(
			name=self.index_prefix,
			body=open("conf/es-template.json").read()
		)
		self.log.info("Put template to ES for pulsar indexes")
		self.last_event_time = time()
		self.index_prefix = self.index_prefix + "-"
		self.index_name = self.get_index_name()
		self.queue = queue
		self.counter = 0
		self.stats_checkpoint = time()
		self.stats_every = 10000
		
		try:
			# This will block as it reads from the queue
			self.bulk(self.es, self.iterator(), stats_only=True)
		except Exception as e:
			self.log.exception("Error with bulk", exc_info=e)

	def bulk(self, client, actions, stats_only=False, **kwargs):
		success, failed = 0, 0

		# list of errors to be collected is not stats_only
		errors = []

		for ok, item in parallel_bulk(client, actions, **kwargs):
			# go through request-reponse pairs and detect failures
			if not ok:
				if not stats_only:
					errors.append(item)
				failed += 1
			else:
				success += 1

		return success, failed if stats_only else errors
	
	def iterator(self):
		for doc in (json.loads(x) for x in iter(self.queue.get, "STOP")):
			self.counter += 1
			if self.counter >= self.stats_every:
				took = time() - self.stats_checkpoint
				rate = float(self.counter) / took
				self.log.info("STATS: rate: %f" % rate)
				self.stats_checkpoint = time()
				self.counter = 0
			yield doc

	def get_index_name(self):
		return "%s%s" % (self.index_prefix, datetime.date.today().isoformat())

class Archiver:
	def __init__(self, queue, conf={}):
		self.conf = conf
		self.log = logging.getLogger("pulsar.archiver")
		if not conf.has_key("directory"):
			raise Exception("No directory given in conf")
		self.directory_folder = conf["directory"]
		with sqlite3.connect(self.conf.get("directory_file", "archive.db")) as con:
			cur = con.cursor()
			cur.execute("CREATE TABLE IF NOT EXISTS directory (id INTEGER UNSIGNED " +
				"PRIMARY KEY, filename VARCHAR(255), start INTEGER UNSIGNED, " +
				"end INTEGER UNSIGNED, count INTEGER UNSIGNED)")
		self.counter = 0
		self.bytes_counter = 0
		self.stats_checkpoint = time()
		self.stats_every = 10000
		self.file_start = time()
		self.batch_limit = conf.get("batch_limit", 10000)
		self.batch_size_limit = conf.get("batch_size_limit", 10 * 1024 * 1024)
		self.queue = queue
		self.current_filename = self.get_new_filename()
		self.out_fh = gzip.GzipFile(self.current_filename, mode="wb")
		for data in iter(self.queue.get, "STOP"):
			data += "\n"
			self.out_fh.write(data)
			self.counter += 1
			self.bytes_counter += len(data)
			if self.counter >= self.stats_every:
				took = time() - self.stats_checkpoint
				rate = float(self.counter) / took
				self.log.info("STATS: rate: %f" % rate)
				self.stats_checkpoint = time()
				self.counter = 0
			if self.counter >= self.batch_limit or self.bytes_counter >= self.batch_size_limit:
				self.rollover()
		
	def get_new_filename(self):
		args = list(datetime.datetime.now().timetuple()[0:6])
		args.insert(0, self.directory_folder)
		folder = "%s/%04d/%02d/%02d/%02d/%02d/%02d" % tuple(args)
		if not os.path.exists(folder):
			os.makedirs(folder)
		filename = "%s.json.gz" % str(uuid.uuid4())
		return "%s/%s" % (folder, filename)

	def rollover(self):
		self.log.info("Rolling over archive file at size %d and count %d" %\
			(self.bytes_counter, self.counter))
		try:
			self.out_fh.close()
			self.add_to_directory()
			self.counter = 0
			self.bytes_counter = 0
			self.current_filename = self.get_new_filename()
			self.out_fh = gzip.GzipFile(self.current_filename, mode="wb")
			self.file_start = time()
		except Exception as e:
			self.log.exception("Error rolling over", exc_info=e)

	def add_to_directory(self):
		with sqlite3.connect(self.conf.get("directory_file", "archive.db")) as con:
			cur = con.cursor()
			cur.execute("INSERT INTO directory (filename, start, end, count) VALUES (?,?,?,?)",
				(self.current_filename, self.file_start, time(), self.counter))
			self.log.debug("Added %s to the directory" % self.current_filename)

class Distributor:
	def __init__(self, conf):
		self.log = logging.getLogger("pulsar.distributor")
		self.log.info("Starting up as user %s" % getpass.getuser())
		self.conf = conf
		
		def spawn_indexer(conf, queue):
			es = Indexer(conf, queue)

		def spawn_archiver(conf, queue):
			try:
				archiver = Archiver(queue, conf)
			except Exception as e:
				self.log.exception("Error starting archiver", exc_info=e)

		self.destinations = []
		if not conf.has_key("passthrough"):
			self.destinations.append({ "queue": Queue() })
			self.destinations[-1]["proc"] = Process(target=spawn_archiver, 
				args=(conf, self.destinations[-1]["queue"]))

			self.destinations.append({ "queue": Queue() })
			self.destinations[-1]["proc"] = Process(target=spawn_indexer, 
				args=(conf, self.destinations[-1]["queue"]))

		self.decorator = Decorator(conf)
		
	def read(self, fh):
		for d in self.destinations:
			d["proc"].start()

		for line in fh:
			try:
				line = json.loads(line)
				line = self.decorator.decorate(line)
				line = json.dumps(line)
			except Exception as e:
				self.log.exception("Error processing line: %s" % (line), exc_info=e)
				continue
			if self.destinations:
				for d in self.destinations:
					d["queue"].put(line)
			else:
				print json.dumps(line)

		if self.destinations:
			for d in self.destinations:
				d["queue"].put("STOP")
				d["proc"].join()

class Decorator:
	def __init__(self, conf={}):
		self.conf = conf
		self.ignore_list = set(["@timestamp", "HOST", "HOST_FROM", "SOURCE", "MESSAGE", "LEGACY_MSGHDR", "PROGRAM", "FILENAME"])
		self.ip_field_list = set(["srcip", "dstip", "ip"])
		self.log = logging.getLogger("pulsar.decorator")
		self.id = uuid.uuid4()
		self.counter = 0

		self.analyze_ips = False
		self.geoip = False
		self._geo_exc = Exception
		try:
			import geoip2.database
			from geoip2.errors import AddressNotFoundError
			self._geo_exc = AddressNotFoundError
			self.geoip = geoip2.database.Reader(conf.get("geoip_db", "/usr/local/share/GeoIP/GeoLite2-City.mmdb"))
			self.analyze_ips = True
		except Exception as e:
			self.log.info("Failed to import geoip2, not using geoip decoration")

		self.stats = {}
		self.stats_every = 1000
		self.last_stat_time = time()
		self.last_stat_counter = 0
		if conf.has_key("ip2asn"):
			self.ip2asn = IP2ASN(conf)
			self.analyze_ips = True
		self.analyze_ips = False
			

	def parse_timestamp(self, timestamp):
		# TODO
		#if timestamp[0] == "2" and timestamp[4] == "-":
		#	# assuming ISO

		return time()

	def log_stats(self):
		time_since_last = time() - self.last_stat_time
		lines_since_last = self.counter - self.last_stat_counter
		ret = {
			"overall_lines_processed": self.counter,
			"lines_processed": lines_since_last,
			"lines_per_second": lines_since_last / float(time_since_last)
		}
		if self.stats.has_key("ip2asn"):
			ret["ip2asn"] = {
				"lookups": self.stats["ip2asn"]["lookups"],
				"db_time_taken": self.stats["ip2asn"]["lookup_time_taken"],
				"avg_db_lookup_time": float(self.stats["ip2asn"]["lookup_time_taken"]) / (self.stats["ip2asn"]["lookups"] - self.stats["ip2asn"]["cache_hits"]),
				"cache_hits": self.stats["ip2asn"]["cache_hits"],
				"cache_hits_percentage": float(self.stats["ip2asn"]["cache_hits"]) / self.stats["ip2asn"]["lookups"]
			}
		
		self.log.debug(json.dumps(ret))
		self.last_stat_time = time()
		self.last_stat_counter = self.counter

	def decorate(self, doc):
		self.counter += 1
		if self.counter % self.stats_every == 0:
			self.log_stats()
		
		# Handle timestamps
		timestamp = doc.get("timestamp", doc.get("@timestamp", time()))
		if type(timestamp) != int or type(timestamp) != float:
			# Epoch will start with 1 until 2035
			if timestamp[0] != "1":
				timestamp = self.parse_timestamp(timestamp)
			else:
				timestamp = float(timestamp)

		if timestamp < 1000000000000:
			# Turn into JS format
			timestamp *= 1000
		# force int from float
		timestamp = int(timestamp)
		
		ret = {
			# It is much cheaper to increment a counter then regen a uuid
			"_id": "%s-%s" % (self.id, self.counter),
			"@message": doc.get("MESSAGE", ""),
			"@timestamp": timestamp,
			"program": doc.get("PROGRAM", ""),
			"header": doc.get("LEGACY_MSGHDR", ""),
			"host": doc.get("HOST", "")
		}
		
		# Cleanup syslog-ng fields
		if doc.has_key("_classifier"):
			ret["class"] = doc["_classifier"]["class"]
			ret["rule_id"] = doc["_classifier"].get("rule_id", 0)
		
		
		for k, v in doc.iteritems():
			if k == "_classifier" or \
				k in self.ignore_list or \
				v == "-":
				continue
			k = k.lower()
			ret[k] = v
		
			if self.analyze_ips and k in self.ip_field_list:
				ipint = struct.unpack("!I", socket.inet_aton(v))[0]
				if rfc1918(ipint):
					ret[k + "_geo"] = {
						"description": "RFC1918"
					}
					continue
				if self.geoip:
					# Attach GeoIP to IP's
					try:
						geoinfo = self.geoip.city(v)
						if geoinfo.country.iso_code != None:
							ret[k + "_geo"] = {
								"cc": geoinfo.country.iso_code,
								"location": {
									"lat": geoinfo.location.latitude,
									"lon": geoinfo.location.longitude
								},
								"accuracy": geoinfo.location.accuracy_radius,
								"state": geoinfo.subdivisions.most_specific.iso_code,
								"city": geoinfo.city.name,
								"country": geoinfo.country.name
							}
					except self._geo_exc:
						pass

				if hasattr(self, "ip2asn"):
					entry = self.ip2asn.lookup(ipint)
					if entry:
						# record = {
						# 	"start": entry[0],
						# 	"end": entry[1],
						# 	"asn": entry[2],
						# 	"description": entry[3]
						# }
						keyname = k + "_geo"
						if not ret.has_key(keyname):
							ret[keyname] = {}
						ret[keyname]["description"] = entry["description"]
						ret[keyname]["asn"] = entry["asn"]
						

						# if self.geoip:
						# 	ret[k + "_geo"].update(entry)
						# 	self.cache.append(entry)
						# else:
						# 	self.cache.append(entry)
						# 	ret[k + "_geo"] = entry
					
				# if self.ip2asn:
				# 	class_c = ipint - (ipint % 256)
				# 	for i, entry in enumerate(self.ip2asn_cache):
				# 		if ipint >= entry["start"] and ipint <= entry["end"]:
				# 			ret[k + "_asn"] = entry
				# 			# Move to top of cache
				# 			self.ip2asn_cache.pop(i)
				# 			self.ip2asn_cache.unshift(entry)
				# 			break
				# 	else:
				# 		try:
				# 			entry = self.ip2asn.execute("SELECT * FROM subnets " +\
				# 				"WHERE class_c=? AND start <= ? AND ? <= end " +\
				# 				"ORDER BY end-start ASC LIMIT 1", 
				# 				(class_c, ipint, ipint)).fetchone()
				# 			if entry:
				# 				record = {
				# 					"start": entry[1],
				# 					"end": entry[2],
				# 					"asn": entry[3],
				# 					"description": self.ip2asn.execute("SELECT description " +\
				# 						"FROM descriptions WHERE id=?", (entry[4],)).fetchone()
				# 				}
				# 				if self.geoip:
				# 					ret[k + "_geo"].update(record)
				# 					self.ip2asn_cache[ entry[1] ] = record
				# 				else:
				# 					ret[k + "_geo"] = self.ip2asn_cache[ entry[1] ] = record
														
				# 		except Exception as e:
				# 			self.log.error("Unable to get IP from %s" % v, exc_info=e)

		return ret


if __name__ == "__main__":
	config = {
		"directory": "/tmp",
		"host": "localhost",
		"port": 9200,
		"log_level": "INFO",
		"log_file": "/var/log/pulsar.log"
	}
	
	if len(sys.argv) > 0:
		config.update(json.load(open(sys.argv[1])))

	if os.environ.has_key("PULSAR_ES_HOST"):
		config["host"] = os.environ["PULSAR_ES_HOST"]
	if os.environ.has_key("PULSAR_ES_PORT"):
		config["port"] = os.environ["PULSAR_ES_PORT"]
	if os.environ.has_key("DEBUG"):
		config["debug"] = True
		config["log_level"] = "DEBUG"
	if os.environ.has_key("PASSTHROUGH"):
		config["passthrough"] = True

	log_options = {
		"format": '%(asctime)s %(name)s %(levelname)s %(process)d %(message)s',
		"level": getattr(logging, config["log_level"].upper())
	}
	if config.has_key("log_file") and not config["log_file"] == "stdout":
		print "setting option"
		log_options["filename"] = config["log_file"]
	logging.basicConfig(**log_options)

	distributor = Distributor(config)
	distributor.read(sys.stdin)
