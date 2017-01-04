import os
import sys
import logging
import sqlite3
import gzip
from time import time

import requests
import ujson as json

class IP2ASN(object):
	def __init__(self, conf):
		self.conf = conf
		self.log = logging.getLogger('pulsar.decorator.ip2asn')
		if os.path.exists(conf["ip2asn"]["db"]):
			self.ip2asn = sqlite3.connect(conf["ip2asn"]["db"]).cursor()
		else:
			#self.ip2asn = self._build_ip2asn()
			self.ip2asn = self._build_ip2asn_class_c()
			#self._build_ip2asn_class_c_sharded()
		self.cache = []
		self.stats = {
			"cache_hits": 0,
			"lookups": 0,
			"lookup_time_taken": 0
		}
		self.cache_limit = 10000
			
	def _build_ip2asn(self):
		# Need to build the DB
		tsv_path = self.conf["ip2asn"].get("tsv", "ip2asn-v4-u32.tsv.gz")
		if not os.path.exists(tsv_path):
			self.log.info("Downloading new ip2asn DB...")
			start = time()
			with open(tsv_path, "w") as fh:
				request = requests.get(self.conf["ip2asn"]["url"], stream=True)
				total_bytes = int(request.headers["Content-Length"])
				bytes_received = 0
				last_reported = None
				for chunk in request.iter_content(chunk_size=32*1024):
					bytes_received += len(chunk)
					fh.write(chunk)
					percent_done = int((float(bytes_received) / total_bytes) * 100)
					if percent_done != last_reported and percent_done % 10 == 0:
						self.log.debug("%d%% complete" % percent_done)
						last_reported = percent_done

			self.log.info("...done. Finished in %f seconds." % (time() - start))
		self.log.info("Building sqlite DB from TSV...")
		db = sqlite3.connect(self.conf["ip2asn"]["db"])
		def dict_factory(cursor, row):
			d = {}
			for idx, col in enumerate(cursor.description):
				d[col[0]] = row[idx]
			return d
		db.row_factory = dict_factory
		# Set autocommit
		
		cur = db.cursor()
		fh = gzip.GzipFile(tsv_path)
		cur.execute("""
CREATE TABLE subnets (
	start INTEGER,
	end INTEGER,
	asn INTEGER,
	description TEXT,
	PRIMARY KEY (start, end)
)
""")
# 		cur.execute("""
# CREATE TABLE descriptions (
# 	id INTEGER PRIMARY KEY,
# 	description TEXT
# )""")

		db.commit()
		#db.isolation_level = None
		sql_params = []
		batch_size = 1000
		for line in fh:
			line = line.rstrip("\n")
			row = line.split("\t")
			asn = int(row[2])
			if asn == 0:
				continue
			start = int(row[0])
			end = int(row[1])
			text = row[4].decode("ascii", "ignore")
			# description_id = cur.execute("SELECT id FROM descriptions " +\
			# 	"WHERE description=?", (text,)).fetchone()
			# if not description_id:
			# 	description_id = cur.execute("INSERT INTO descriptions " +\
			# 		"(description) VALUES (?)", (text,)).lastrowid
			# else:
			# 	description_id = description_id[0]
			# sql_params.append((start, end, asn, description_id))
			sql_params.append((start, end, asn, text))
			if len(sql_params) > batch_size:
				self.log.debug("Committing %d entries..." % batch_size)
				try:
					cur.executemany("INSERT INTO subnets " +\
						"(start, end, asn, description) VALUES (?,?,?,?)",
						sql_params
					)
					sql_params = []
				except sqlite3.IntegrityError:
					pass
		# Insert leftovers
		if len(sql_params):
			try:
				cur.executemany("INSERT INTO subnets " +\
					"(start, end, asn, description) VALUES (?,?,?,?)",
					sql_params
				)
			except sqlite3.IntegrityError:
				pass


		db.commit()
		self.log.debug("Finished building DB.")
		return db.cursor()

	def _build_ip2asn_class_c_sharded(self):
		# Need to build the DB
		tsv_path = self.conf["ip2asn"].get("tsv", "ip2asn-v4-u32.tsv.gz")
		if not os.path.exists(tsv_path):
			self.log.info("Downloading new ip2asn DB...")
			start = time()
			with open(tsv_path, "w") as fh:
				request = requests.get(self.conf["ip2asn"]["url"], stream=True)
				total_bytes = int(request.headers["Content-Length"])
				bytes_received = 0
				last_reported = None
				for chunk in request.iter_content(chunk_size=32*1024):
					bytes_received += len(chunk)
					fh.write(chunk)
					percent_done = int((float(bytes_received) / total_bytes) * 100)
					if percent_done != last_reported and percent_done % 10 == 0:
						self.log.debug("%d%% complete" % percent_done)
						last_reported = percent_done

			self.log.info("...done. Finished in %f seconds." % (time() - start))
		self.log.info("Building sqlite DB from TSV...")
		shard_count = 1
		counter = 0
		with gzip.GzipFile(tsv_path) as fh:
			for line in fh:
				row = line.split("\t")
				asn = int(row[2])
				if asn == 0:
					continue
				start = int(row[0])
				end = int(row[1])
				class_c = start - (start % 256)
				if end - start <= 256:
					counter += 1
				else:
					i = class_c
					while i < end:
						counter += 1		
						i += 256
				
		self.log.debug(("TSV contains %d non-zero entries, splitting into %d " + \
			"shards making %d entries per shard") % (counter, shard_count, counter/shard_count))
		shards = []
		def dict_factory(cursor, row):
			d = {}
			for idx, col in enumerate(cursor.description):
				d[col[0]] = row[idx]
			return d
		for i in range(shard_count):
			db = sqlite3.connect(self.conf["ip2asn"]["db"] + ("_%d" % i))
			db.row_factory = dict_factory
			shards.append((db, db.cursor()))
			cur = db.cursor()
			cur.execute("""
	CREATE TABLE subnets (
		class_c INTEGER,
		start INTEGER,
		end INTEGER,
		asn INTEGER,
		description TEXT,
		PRIMARY KEY (class_c, start)
	)
	""")
		self.log.debug("shards: %d, %r" % (len(shards), shards))
		fh = gzip.GzipFile(tsv_path)
		sql_params = []
		batch_size = 100000
		shard_boundary_size = (counter / shard_count) + 1
		inserted = 0
		for line in fh:
			line = line.rstrip("\n")
			row = line.split("\t")
			asn = int(row[2])
			if asn == 0:
				continue
			start = int(row[0])
			end = int(row[1])
			text = row[4].decode("ascii", "ignore")
			class_c = start - (start % 256)
			if end - start <= 256:
				sql_params.append((class_c, start, end, asn, text))
				inserted += 1
			else:
				i = class_c
				while i < end:
					sql_params.append((i, start, end, asn, text))
					inserted += 1
					i += 256
			if len(sql_params) >= batch_size:
				# Determine shard based on boundary of counter
				shard = inserted / shard_boundary_size
				self.log.debug("Committing %d entries to shard %d..." % (batch_size, shard))
				db, cur = shards[shard]
				try:
					cur.executemany("INSERT INTO subnets " +\
						"(class_c, start, end, asn, description) VALUES (?,?,?,?,?)",
						sql_params
					)
					sql_params = []
				except sqlite3.IntegrityError:
					pass
				except sqlite3.InterfaceError:
					pass
		if len(sql_params):
			db, cur = shards[-1]
			try:
				cur.executemany("INSERT INTO subnets " +\
					"(class_c, start, end, asn, description) VALUES (?,?,?,?,?)",
					sql_params
				)
			except sqlite3.IntegrityError:
				pass
			except sqlite3.InterfaceError:
				pass
		self.shards = []
		for db, cur in shards:
			db.commit()
			minmax = cur.execute("SELECT MIN(class_c) AS min, MAX(class_c) AS max FROM subnets").fetchone()
			self.shards.append((db, cur, minmax["min"], minmax["max"]))
		self.log.debug("Finished building DB.")
		
	def _build_ip2asn_class_c(self):
		# Need to build the DB
		tsv_path = self.conf["ip2asn"].get("tsv", "ip2asn-v4-u32.tsv.gz")
		if not os.path.exists(tsv_path):
			self.log.info("Downloading new ip2asn DB...")
			start = time()
			with open(tsv_path, "w") as fh:
				request = requests.get(self.conf["ip2asn"]["url"], stream=True)
				total_bytes = int(request.headers["Content-Length"])
				bytes_received = 0
				last_reported = None
				for chunk in request.iter_content(chunk_size=32*1024):
					bytes_received += len(chunk)
					fh.write(chunk)
					percent_done = int((float(bytes_received) / total_bytes) * 100)
					if percent_done != last_reported and percent_done % 10 == 0:
						self.log.debug("%d%% complete" % percent_done)
						last_reported = percent_done

			self.log.info("...done. Finished in %f seconds." % (time() - start))
		self.log.info("Building sqlite DB from TSV...")
		db = sqlite3.connect(self.conf["ip2asn"]["db"])
		def dict_factory(cursor, row):
			d = {}
			for idx, col in enumerate(cursor.description):
				d[col[0]] = row[idx]
			return d
		db.row_factory = dict_factory
		cur = db.cursor()
		fh = gzip.GzipFile(tsv_path)
		cur.execute("""
CREATE TABLE subnets (
	class_c INTEGER,
	start INTEGER,
	end INTEGER,
	asn INTEGER,
	description TEXT,
	PRIMARY KEY (class_c, start)
)
""")
# 		cur.execute("""
# CREATE TABLE descriptions (
# 	id INTEGER PRIMARY KEY,
# 	description TEXT
# )""")
		sql_params = []
		batch_size = 100000
		for line in fh:
			line = line.rstrip("\n")
			row = line.split("\t")
			asn = int(row[2])
			if asn == 0:
				continue
			start = int(row[0])
			end = int(row[1])
			text = row[4].decode("ascii", "ignore")
			# description_id = cur.execute("SELECT id FROM descriptions " +\
			# 	"WHERE description=?", (text,)).fetchone()
			# if not description_id:
			# 	description_id = cur.execute("INSERT INTO descriptions " +\
			# 		"(description) VALUES (?)", (text,)).lastrowid
			# else:
			# 	description_id = description_id[0]
			class_c = start - (start % 256)
			if end - start <= 256:
				sql_params.append((class_c, start, end, asn, text))
			else:
				i = class_c
				while i < end:
					sql_params.append((i, start, end, asn, text))
					i += 256
			if len(sql_params) >= batch_size:
				self.log.debug("Committing %d entries..." % batch_size)
				try:
					cur.executemany("INSERT INTO subnets " +\
						"(class_c, start, end, asn, description) VALUES (?,?,?,?,?)",
						sql_params
					)
					sql_params = []
				except sqlite3.IntegrityError:
					pass
				except sqlite3.InterfaceError:
					pass
		if len(sql_params):
			try:
				cur.executemany("INSERT INTO subnets " +\
					"(class_c, start, end, asn, description) VALUES (?,?,?,?,?)",
					sql_params
				)
			except sqlite3.IntegrityError:
				pass
			except sqlite3.InterfaceError:
				pass
		db.commit()
		self.log.debug("Finished building DB.")
		return db.cursor()

	def _lookup(self, ipint):
		self.stats["lookups"] += 1
		for i, entry in enumerate(self.cache):
			if ipint >= entry["start"] and ipint <= entry["end"]:
				self.stats["cache_hits"] += 1
				if i != 0:
					# Move to top of cache
					self.cache.pop(i)
					self.cache.insert(0, entry)
				return entry
			else:
				# Find the shard
				for db, cur, min_c, max_c in self.shards:
					if ipint < min_c or ipint > max_c:
						continue
					try:
						start_time = time()
						entry = cur.execute("SELECT * FROM subnets " +\
							"WHERE class_c=? AND start <= ? AND ? <= end " +\
							"ORDER BY end-start ASC LIMIT 1", 
							(ipint - (ipint % 256), ipint, ipint)).fetchone()
						# entry = self.ip2asn.execute("SELECT * FROM subnets " +\
						# 	"WHERE start <= ? AND ? <= end " +\
						# 	"ORDER BY end-start ASC LIMIT 1", 
						# 	(ipint, ipint)).fetchone()
						took = time() - start_time
						self.stats["lookup_time_taken"] += took
						if entry:
							self.cache.insert(0, entry)
							if len(self.cache) > self.cache_limit:
								self.cache.pop(len(self.cache) - 1)
						return entry
						
													
					except Exception as e:
						self.log.exception("Error during ip2asn", exc_info=e)

	def lookup(self, ipint):
		self.stats["lookups"] += 1
		for i, entry in enumerate(self.cache):
			if ipint >= entry["start"] and ipint <= entry["end"]:
				self.stats["cache_hits"] += 1
				if i != 0:
					# Move to top of cache
					self.cache.pop(i)
					self.cache.insert(0, entry)
				return entry
			else:
				try:
					start_time = time()
					entry = self.ip2asn.execute("SELECT * FROM subnets " +\
						"WHERE class_c=? AND start <= ? AND ? <= end " +\
						"ORDER BY end-start ASC LIMIT 1", 
						(ipint - (ipint % 256), ipint, ipint)).fetchone()
					# entry = self.ip2asn.execute("SELECT * FROM subnets " +\
					# 	"WHERE start <= ? AND ? <= end " +\
					# 	"ORDER BY end-start ASC LIMIT 1", 
					# 	(ipint, ipint)).fetchone()
					took = time() - start_time
					self.stats["lookup_time_taken"] += took
					if entry:
						self.cache.insert(0, entry)
						if len(self.cache) > self.cache_limit:
							self.cache.pop(len(self.cache) - 1)
					return entry
					
												
				except Exception as e:
					self.log.exception("Error during ip2asn", exc_info=e)