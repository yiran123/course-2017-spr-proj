import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from geopy.distance import vincenty
from helper import *

class example(dml.Algorithm):
    contributor = 'wuhaoyu_yiran123'
    reads = []
    writes = ['wuhaoyu_yiran123.universityLocation', 'wuhaoyu_yiran123.mbtaStops', 'wuhaoyu_yiran123.trafficJam', 'wuhaoyu_yiran123.trafficLightLocation', 'wuhaoyu_yiran123.roadLocation']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('wuhaoyu_yiran123', 'wuhaoyu_yiran123')


        # University location
        url = 'http://datamechanics.io/data/Colleges_and_Universities.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['features']
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("universityLocation")
        repo.createCollection("universityLocation")
        repo['wuhaoyu_yiran123.universityLocation'].insert_many(r)
        repo['wuhaoyu_yiran123.universityLocation'].metadata({'complete':True})
        print(repo['wuhaoyu_yiran123.universityLocation'].metadata())

        # Boston mbta stops
        url = 'http://datamechanics.io/data/wuhaoyu_yiran123/MBTA_Bus_Stops.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['features']
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("mbtaStops")
        repo.createCollection("mbtaStops")
        repo['wuhaoyu_yiran123.mbtaStops'].insert_many(r)
        repo['wuhaoyu_yiran123.universityLocation'].metadata({'complete': True})
        print(repo['wuhaoyu_yiran123.universityLocation'].metadata())

        # traffic jam
        url = 'http://datamechanics.io/data/wuhaoyu_yiran123/trafficJam.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("trafficJam")
        repo.createCollection("trafficJam")
        repo['wuhaoyu_yiran123.trafficJam'].insert_many(r)
        repo['wuhaoyu_yiran123.universityLocation'].metadata({'complete': True})
        print(repo['wuhaoyu_yiran123.universityLocation'].metadata())


        # traffic light location
        url = 'http://datamechanics.io/data/wuhaoyu_yiran123/Traffic_Signals.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['features']
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("trafficLightLocation")
        repo.createCollection("trafficLightLocation")
        repo['wuhaoyu_yiran123.trafficLightLocation'].insert_many(r)
        repo['wuhaoyu_yiran123.universityLocation'].metadata({'complete': True})
        print(repo['wuhaoyu_yiran123.universityLocation'].metadata())

        # road Location
        url = 'http://datamechanics.io/data/wuhaoyu_yiran123/ex_MRywx7UGz9G6a4Kftj3Rh4Svejuu3_roads_gen0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)['features']
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("roadLocation")
        repo.createCollection("roadLocation")
        repo['wuhaoyu_yiran123.roadLocation'].insert_many(r)
        repo['wuhaoyu_yiran123.universityLocation'].metadata({'complete': True})
        print(repo['wuhaoyu_yiran123.universityLocation'].metadata())

        endTime = datetime.datetime.now()

        def getCollection(db):
            temp=[]
            for i in repo['wuhaoyu_yiran123.'+db].find():
                temp.append(i)
            return temp

        #Get the University name, latitude and longtitude in city of Boston
        s=getCollection('universityLocation')
        school=[]
        for i in s:
            if i['properties']['Latitude'] == 0 or i['properties']['Longitude'] == '0':
                continue
            else:
                school.append((i['properties']['Name'],i['properties']['NumStudent'],(i['properties']['Latitude'],i['properties']['Longitude'])))
            # school.append({'name':i['properties']['Name'],'latitude':i['properties']['Latitude'], 'longitude':i['properties']['Longitude']})
        # print (school)

        #Get the traffic jam street name, the start time
        j=getCollection('trafficJam')
        jam=[]
        for i in j:
            if i['street']==None or (i['street'], i['startTime']) in jam:
                continue
            else:
                jam.append((i['street'], i['startTime']))

        #Get the street name and its corresponding longtitude and latitude
        r=getCollection('roadLocation')
        road=[]
        roadName=[]
        for i in r:
            if i["properties"]['name'] in roadName:
                continue
            else:
                roadName.append(i["properties"]['name'])
                road.append((i["properties"]['name'],i['geometry']['coordinates'][0][1], i['geometry']['coordinates'][0][0]))

        #get the roadLocation and trafficJamLocation to get the coordinates
        tlocation=project(select(product(jam,road), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1], t[1][1], t[1][2]))

        m=getCollection('mbtaStops')
        mbta=[]
        for i in m:
            if i['geometry']['coordinates'][0] == 0 or i['geometry']['coordinates'][1] == '0':
                continue
            else:
                mbta.append((i['geometry']['coordinates'][1],i['geometry']['coordinates'][0]))
        # print (mbta)

        l=getCollection('trafficLightLocation')
        lights=[]
        for i in l:
            if i['geometry']['coordinates'][0] == 0 or i['geometry']['coordinates'][1] == '0':
                continue
            else:
                lights.append((i['geometry']['coordinates'][1],i['geometry']['coordinates'][0]))

        lightDistance = product(school, lights)
        for i in range(len(lightDistance)):
            lightDistance[i]=(lightDistance[i],(vincenty((lightDistance[i][0][2]),(lightDistance[i][1])).miles))

        mbtaDistance = product(school, mbta)
        for i in range(len(mbtaDistance)):
            mbtaDistance[i]=(mbtaDistance[i],(vincenty((mbtaDistance[i][0][2]),(mbtaDistance[i][1])).miles))

        jamDistance = product(school, tlocation)
        for i in range(len(jamDistance)):
            jamDistance[i]=(jamDistance[i],(vincenty(jamDistance[i][0][2], (jamDistance[i][1][2], jamDistance[i][1][3])).miles))

        j0=map(lambda k,v: [((k,v), (k,v))] if v < 2 else [], jamDistance)
        print(j0[0])
        jamRange = reduce(lambda k,vs: k, j0)

        l0=map(lambda k,v: [((k,v), (k,v))] if v < 2 else [], lightDistance)
        lightRange = reduce(lambda k,vs: k, l0)

        m0=map(lambda k,v: [((k,v), (k,v))] if v < 2 else [], mbtaDistance)
        mbtaRange = reduce(lambda k,vs: k, m0)

        lightMerge = aggregate()

        repo.logout()
        # return {"start":startTime, "end":endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('wuhaoyu_yiran123', 'wuhaoyu_yiran123')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:wuhaoyu_yiran123#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'University Location and Transportation DataSets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_universityLocation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_mbtaStops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_trafficJam = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_trafficLightLocation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_roadLocation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_universityLocation, this_script)
        doc.wasAssociatedWith(get_mbtaStops, this_script)
        doc.wasAssociatedWith(get_trafficJam, this_script)
        doc.wasAssociatedWith(get_trafficLightLocation, this_script)
        doc.wasAssociatedWith(get_roadLocation, this_script)

        doc.usage(get_universityLocation, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_mbtaStops, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_trafficJam, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_trafficLightLocation, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_roadLocation, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )


        universityLocation = doc.entity('dat:wuhaoyu_yiran123#universityLocation', {prov.model.PROV_LABEL:'Univeristy Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(universityLocation, this_script)
        doc.wasGeneratedBy(universityLocation, get_universityLocation, endTime)
        doc.wasDerivedFrom(universityLocation, resource, get_universityLocation, get_universityLocation, get_universityLocation)

        mbtaStops = doc.entity('dat:wuhaoyu_yiran123#mbtaStops', {prov.model.PROV_LABEL:'MBTA Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbtaStops, this_script)
        doc.wasGeneratedBy(mbtaStops, get_mbtaStops, endTime)
        doc.wasDerivedFrom(mbtaStops, resource, get_mbtaStops, get_mbtaStops, get_mbtaStops)

        trafficJam = doc.entity('dat:wuhaoyu_yiran123#trafficJam', {prov.model.PROV_LABEL:'Traffic Jams', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trafficJam, this_script)
        doc.wasGeneratedBy(trafficJam, get_trafficJam, endTime)
        doc.wasDerivedFrom(trafficJam, resource, get_trafficJam, get_trafficJam, get_trafficJam)

        trafficLightLocation = doc.entity('dat:wuhaoyu_yiran123#trafficLightLocation', {prov.model.PROV_LABEL:'Traffic Lights Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trafficLightLocation, this_script)
        doc.wasGeneratedBy(trafficLightLocation, get_trafficLightLocation, endTime)
        doc.wasDerivedFrom(trafficLightLocation, resource, get_trafficLightLocation, get_trafficLightLocation, get_trafficLightLocation)

        roadLocation = doc.entity('dat:wuhaoyu_yiran123#roadLocation', {prov.model.PROV_LABEL:'Traffic Lights Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(roadLocation, this_script)
        doc.wasGeneratedBy(roadLocation, get_roadLocation, endTime)
        doc.wasDerivedFrom(roadLocation, resource, get_roadLocation, get_roadLocation, get_roadLocation)

        repo.logout()

        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
