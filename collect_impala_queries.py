import datetime
import json
import sys
import requests

if len(sys.argv) != 4:
  print("Usage: START_DATE_HOUR END_DATE_HOUR OUTPUT_JSON_FILE")
  print("Example: 2020-01-02-13 2020-01-02-14 output.json")
  sys.exit(1)

start_hour = [ int(i) for i in sys.argv[1].split("-") ]
end_hour = [ int(i) for i in sys.argv[2].split("-") ]
output_file = sys.argv[3]

date_from = datetime.datetime(start_hour[0], start_hour[1], start_hour[2], start_hour[3], 0).isoformat()
date_to = datetime.datetime(end_hour[0], end_hour[1], end_hour[2], end_hour[3], 0).isoformat()

cm_protocol = "http"
cm_port = "7180"
cm_api_version = "v19"
cm_api_cluster_name = "nameservice1"
cm_api_impala_name = "impala"

cm_hostname = "172.31.0.58"
cm_user = "admin"
cm_password = "admin"

#api_url = '%s://%s:%s/api/%s/clusters/%s/services/impala/impalaQueries?from=%sZ&to=%sZ&filter=&limit=100&filter=(executing=false)' % (cm_protocol, cm_hostname, cm_port, cm_a
pi_version, cm_api_cluster_name, date_from, date_to)
api_url = '%s://%s:%s/api/%s/clusters/%s/services/impala/impalaQueries?from=%sZ&to=%sZ&limit=1000' % (cm_protocol, cm_hostname, cm_port, cm_api_version, cm_api_cluster_name, 
date_from, date_to)
queries = []
offset = 0
while True:
  print("Offset: " + str(offset))

  tmp_url = api_url + '&offset=' + str(offset)
  print(tmp_url)
  offset += 100

  response = requests.get(tmp_url, auth=(cm_user, cm_password), verify=False)
  if response.status_code != 200:
    exit(response.text)

  response_json = json.loads(response.text)
  if not response_json['queries']:
    # we have ran out of pages to go through
    break

  print("Results: " + str(len(response_json['queries'])))
  queries += response_json['queries']

json_output = {}
for query in queries:
  qid = query["queryId"]
  json_output[qid] = query
print(json_output)

with open(output_file, "w") as fp:
  json.dump(json_output, fp)
