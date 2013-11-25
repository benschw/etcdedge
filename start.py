#!/usr/bin/python

#jsonStr='{"action":"get","key":"/svc","dir":true,"kvs":[{"key":"/svc/memcache","dir":true,"kvs":[{"key":"/svc/memcache/9c516dcb11b5","value":"172.17.42.1:49268","expiration":"2013-11-19T14:36:19.749631144Z","ttl":5,"modifiedIndex":6}],"modifiedIndex":2},{"key":"/svc/firstbase-authserver","dir":true,"kvs":[{"key":"/svc/firstbase-authserver/d8f517740aa0","value":"172.17.42.1:49269","expiration":"2013-11-19T14:36:17.980095404Z","ttl":3,"modifiedIndex":5}],"modifiedIndex":5}],"modifiedIndex":2}'


import json,os,sys,redis,urllib2,time
import logging


if "DEBUG" in os.environ:
	logging.basicConfig(level=logging.DEBUG)


# in a but not b
def diff(a, b):
	b = set(b)
	return [aa for aa in a if aa not in b]

def intersect(a, b):
	return list(set(a) & set(b))



class Service:

	def __init__(self, id, host, instances):
		self.id = id
		self.host = host
		self.instances = instances


def updateProxy(svc, rs):
	if svc.host is not None:
		key = "frontend:"+svc.host

		if not rs.exists(key):
			rs.rpush(key, svc.id)

		#rs.expire(key, 60)

		existing = rs.lrange(key, 0, -1)

		current = svc.instances
		current.insert(0, svc.id)

		toDelete = diff(existing, current)
		toAdd = diff(current, existing)

		for val in toDelete:
			logging.info("D "+key+" "+val)
			rs.lrem(key, val)

		for val in toAdd:
			logging.info("A "+key+" "+val)
			rs.rpushx(key, val)

def getServiceFromVo(ident, keys):
	host = None
	instances = []
	for k in keys:
		logging.info(k['key'])

		if k['key'] == "/svc/"+ident+"/name" and k['key'] != "":
			logging.info(k['value'])
			host = k['value']

		if k['key'] == "/svc/"+ident+"/instances" and 'kvs' in k:
			for instance in k['kvs']:
				logging.info(instance['value'])

				instances.append("http://"+str(instance["value"]))

	return Service(ident, host, instances)


	

sys.stderr.write("connecting to "+os.environ['REDIS'])


opener = urllib2.build_opener(urllib2.HTTPHandler)
request = urllib2.Request("http://"+os.environ['ETCD']+"/v2/keys/svc/")
request.get_method = lambda: 'PUT'
url = opener.open(request)


redisAdd = os.environ['REDIS'].split(":")


rs = redis.Redis(redisAdd[0], int(redisAdd[1]))

while True:
	try:
		jsonStr = urllib2.urlopen("http://"+os.environ['ETCD']+"/v2/keys/svc/?recursive=true").read()
		obj = json.loads(jsonStr)


		if "kvs" in obj:
			for svc in obj["kvs"]:
				svcKey = svc["key"]
				parts = svcKey.split("/")

				ident = parts[2];

				if "kvs" in svc:
					logging.info(ident)

					svc = getServiceFromVo(ident, svc['kvs'])
					
					updateProxy(svc, rs)

	
	except Exception as e:
		print e
	
	time.sleep(5)

logging.info("exiting...")
