import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from geopy.distance import vincenty #need to install geopy first
from helper import *

class schoolTrafficLights(dml.Algorithm):
    contributor = 'wuhaoyu_yiran123'
    reads = ['wuhaoyu_yiran123.universityLocation', 'wuhaoyu_yiran123.trafficLightLocation']
    writes = ['wuhaoyu_yiran123.schoolTrafficLights']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

            # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('wuhaoyu_yiran123', 'wuhaoyu_yiran123')

        def getCollection(db):
            temp=[]
            for i in repo['wuhaoyu_yiran123.'+db].find():
                temp.append(i)
            return temp

        #Get the University name, latitude and longtitude in city of Boston
        s=getCollection('universityLocation')
        school=[]
        for i in s:
            if i['properties']['Latitude'] == 0.0 or i['properties']['Longitude'] == 0.0 or i['properties']['NumStudent'] == 0.0:
                continue
            else:
                school.append((i['properties']['Name'], (i['properties']['Latitude'],i['properties']['Longitude'])))

        #Get the traffic light name, latitude and longtitude in city of Boston
        l=getCollection('trafficLightLocation')
        lights=[]
        for i in l:
            if i['geometry']['coordinates'][0] == 0.0 or i['geometry']['coordinates'][1] == 0.0:
                continue
            else:
                lights.append((i['properties']['Location'], (i['geometry']['coordinates'][1],i['geometry']['coordinates'][0])))

        #Merge the data of school and the data of traffic lights
        lightDistance = product(school, lights)

        #Calculate the distance of the traffic lights and university locations
        for i in range(len(lightDistance)):
            lightDistance[i]=(lightDistance[i],(vincenty((lightDistance[i][0][1]),(lightDistance[i][1][1])).miles))

        l0=map(lambda k,v: [((k,v), (k,v))] if v < 2 else [], lightDistance)
        lightRange = reduce(lambda k,vs: k, l0)

        repo.dropCollection("schoolTrafficLights")
        repo.createCollection("schoolTrafficLights")

        for i in lightRange:
            insertMaterial = {'universityName': i[0][0], 'trafficLightName': i[0][1], 'distance': i[1]}
            repo["wuhaoyu_yiran123.schoolTrafficLights"].insert_one(insertMaterial)


        repo.logout()
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

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

        this_script = doc.agent('alg:wuhaoyu_yiran123#schoolTrafficLights', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label': 'Find all the traffic lights that are 2 miles within each university and show the distance between each light and the univeristy'})

        doc.wasAssociatedWith(this_script, this_run)

        get_universityLocation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_mbtaStops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_trafficJam = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_trafficLightLocation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_roadLocation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.usage(get_universityLocation, this_run, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_trafficLightLocation, this_run, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )


        universityLocation = doc.entity('dat:wuhaoyu_yiran123#universityLocation', {prov.model.PROV_LABEL:'Univeristy Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(universityLocation, this_script)
        doc.wasGeneratedBy(universityLocation, this_run, endTime)
        doc.wasDerivedFrom(universityLocation, get_universityLocation, this_run, this_run, this_run)

        trafficLightLocation = doc.entity('dat:wuhaoyu_yiran123#trafficLightLocation', {prov.model.PROV_LABEL:'Traffic Lights Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trafficLightLocation, this_script)
        doc.wasGeneratedBy(trafficLightLocation, this_run, endTime)
        doc.wasDerivedFrom(trafficLightLocation, get_trafficLightLocation, this_run, this_run, this_run)

        repo.logout()

        return doc


"""trafficJamCoordination.execute()
doc = trafficJamCoordination.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))"""

## eof
