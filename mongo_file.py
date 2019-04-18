import requests,json

#function that send data to a particular collection in mongo_db
def MongoDB(data, collection):
    params = {'apiKey': 'atdvvjACxH_EPM4jb6wlUip_jvONc0Tu'}
    dbname='mongo'
    data2 = json.dumps(data)
    data2 = json.loads(data2)
    url = 'https://api.mlab.com/api/1/databases/' + dbname + '/collections/' + collection
    data3 = json.dumps(data2)
    headers = {'content-type': 'application/json'}
    response = requests.post(url, data=data3, params=params, headers=headers)
    response.text