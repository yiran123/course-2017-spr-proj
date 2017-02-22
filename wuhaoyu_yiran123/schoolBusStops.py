import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from geopy.distance import vincenty #need to install geopy first
from helper import *

class schoolBusStops(dml.Algorithm):
    contributor = 'wuhaoyu_yiran123'
    reads = ['wuhaoyu_yiran123.universityLocation', 'wuhaoyu_yiran123.mbtaStops']
    writes = ['wuhaoyu_yiran123.schoolBusStops']

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
                school.append((i['properties']['Name'], i['properties']['NumStudent'], (i['properties']['Latitude'], i['properties']['Longitude'])))

        #Get the mbta bus stop name and its coordination
        m=getCollection('mbtaStops')
        mbta=[]
        for i in m:
            if i['geometry']['coordinates'][0] == 0.0 or i['geometry']['coordinates'][1] == 0.0:
                continue
            else:
                mbta.append((i['properties']['STOP_NAME'], (i['geometry']['coordinates'][1],i['geometry']['coordinates'][0])))

        #Merge the data of school and the data of bus stops
        mbtaDistance = product(school, mbta)

        #Calculate the distance of the traffic locations and university locations
        for i in range(len(mbtaDistance)):
            mbtaDistance[i]=(mbtaDistance[i],(vincenty((mbtaDistance[i][0][2]),(mbtaDistance[i][1][1])).miles))

        #Count the bus stops that within the 2 miles range of each university
        busRange = select(mbtaDistance, lambda t: t[1] <= 2)
        busRange1 = project(busRange, lambda t:(t[0][0], 1))
        busRange2 = aggregate(busRange1, sum)

        """for i in lightRange:
            insertMaterial = {'universityName': i[0][0], '': i[0][1], 'distance': i[1]}
            repo["wuhaoyu_yiran123.schoolTrafficLights"].insert_one(insertMaterial)"""

        repo.drop_collection('schoolBusStops')
        repo.create_collection('schoolBusStops')

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

        this_script = doc.agent('alg:wuhaoyu_yiran123#schoolBusStops', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label': 'Find all the bus stops for each university'})

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
        doc.usage(get_mbtaStops, this_run, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )


        universityLocation = doc.entity('dat:wuhaoyu_yiran123#universityLocation', {prov.model.PROV_LABEL:'Univeristy Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(universityLocation, this_script)
        doc.wasGeneratedBy(universityLocation, this_run, endTime)
        doc.wasDerivedFrom(universityLocation, get_universityLocation, this_run, this_run, this_run)

        mbtaStops = doc.entity('dat:wuhaoyu_yiran123#mbtaStops', {prov.model.PROV_LABEL:'MBTA Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbtaStops, this_script)
        doc.wasGeneratedBy(mbtaStops, this_run, endTime)
        doc.wasDerivedFrom(mbtaStops, get_mbtaStops, this_run, this_run, this_run)

        repo.logout()

        return doc


"""schoolBusStops.execute()
doc = schoolBusStops.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))"""

## eof
