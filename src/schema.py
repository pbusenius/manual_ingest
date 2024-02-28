import json
import requests

BASE_URL = "http://localhost:18081"

data = """
    {
		"type": "record",
		"name": "message",
		"namespace": "de.ais.avro",
		"fields" : [
			{"name": "MMSI", "type": "string"},
			{"name": "TIMESTAMPUTC", "type": "string"},
			{"name": "MESSAGEID", "type": "string"},
			{"name": "LONGITUDE", "type": "double"},
			{"name": "LATITUDE", "type": "double"},
			{"name": "SOG", "type": "double"},
			{"name": "COG", "type": "double"}
		]
	}
"""

res = requests.post(
    url=f'{BASE_URL}/subjects/ais-input/versions',
    data=json.dumps({
      'schema': json.dumps(json.loads(data))
    }),
    headers={'Content-Type': 'application/vnd.schemaregistry.v1+json'}).json()

print(res)