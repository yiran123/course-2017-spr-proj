import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from helper import *

class trafficJamCoordination(dml.Algorithm):
    contributor = 'wuhaoyu_yiran123'
    reads = ['wuhaoyu_yiran123.trafficJam', 'wuhaoyu_yiran123.roadLocation']
    writes = ['wuhaoyu_yiran123.trafficJamCoordination']

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


        #Get the traffic jam street name, the start time
        j=getCollection('trafficJam')
        jam=[]
        for i in j:
            if i['street']== None or (i['street'], i['startTime']) in jam:
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
                road.append((i["properties"]['name'],i['geometry']['coordinates'][0][0], i['geometry']['coordinates'][0][1]))


        #get the roadLocation and trafficJamLocation to get the coordinates
        tlocation=project(select(product(jam,road), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1], t[1][1], t[1][2]))

        #Create the new dataset
        repo.drop_collection("universityTraffic")
        repo.create_collection("universityTraffic")

        #Add data into the dataset
        for i in tlocation:
            insertMaterial = {'streetName':i[0], 'jamStartTime':i[1], 'coordination':(i[2], i[3])}
            repo["wuhaoyu_yiran123.universityTraffic"].insert_one(insertMaterial)


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

        this_script = doc.agent('alg:wuhaoyu_yiran123#trafficJamCoordination', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label': 'Merge the traffic jam data with the road coordination'})

        doc.wasAssociatedWith(this_script, this_run)

        get_universityLocation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_mbtaStops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_trafficJam = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_trafficLightLocation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_roadLocation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.usage(get_trafficJam, this_run, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_roadLocation, this_run, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        trafficJam = doc.entity('dat:wuhaoyu_yiran123#trafficJam', {prov.model.PROV_LABEL:'Traffic Jams', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trafficJam, this_script)
        doc.wasGeneratedBy(trafficJam, this_run, endTime)
        doc.wasDerivedFrom(trafficJam, get_trafficJam, this_run, this_run, this_run)

        roadLocation = doc.entity('dat:wuhaoyu_yiran123#roadLocation', {prov.model.PROV_LABEL:'Traffic Lights Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(roadLocation, this_script)
        doc.wasGeneratedBy(roadLocation, this_run, endTime)
        doc.wasDerivedFrom(roadLocation, get_roadLocation, this_run, this_run, this_run)

        repo.logout()

        return doc


"""trafficJamCoordination.execute()
doc = trafficJamCoordination.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))"""

## eof
