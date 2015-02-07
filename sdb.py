import http.client
import json

class Client:
	def __init__(self, address):
		self.connection = http.client.HTTPConnection(address)

	def sources(self, start=-86400, end=0):
		self.connection.request('GET', '/sources?start=' + 
			str(start) + '&end=' + str(end))
		return json.loads(self.connection.getresponse().read().decode())

	def metrics(self, source, start=-86400, end=0):
		self.connection.request('GET', '/source/'+source+'/metrics?start=' + 
			str(start) + '&end=' + str(end))
		return json.loads(self.connection.getresponse().read().decode())

	def insert(self, rows):
		self.connection.request('POST', '/insert', json.dumps(rows))
		response = self.connection.getresponse()
		response.read()
		if response.status is not 200:
			return False

		return True

	def query(self, querydescs, downsample=0):
		self.connection.request('GET', '/query?downsample=' +
			str(downsample), json.dumps(querydescs))
		return json.loads(self.connection.getresponse().read().decode())
