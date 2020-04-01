from google.cloud import firestore
import google.cloud.exceptions

from cloudsecrets.gcp import Secrets
import json

PROJECT = "imposing-union-227917"

db = firestore.Client(project=PROJECT)
dpm = Secrets("dpm-secrets", project=PROJECT)

services = json.loads(dict(dpm).get("services","{}"))

key_list = []
for svc in services.keys():
    programs = services.get(svc).get("programs","{}").keys()
    if len(programs) > 0:
        for prg in programs:
            key_list += [f"dpm-{svc}-{prg}-config"]

#
# load the configs into firebase
#
for key in key_list:
    s = Secrets(key)
    for k,v in dict(s).items():
        try:
            d = json.loads(v)
        except:
            d = { "value": v, "description": "" }
        if not type(d) == dict:
            d = { "value": d, "description": "" }
        ref = db.collection(key).document(k)
        ref.set(d)

#
# retrieve keys from firestore
#
for key in key_list:
    ref = db.collection(key)
    print(ref)
    docs = ref.stream()
    for doc in docs:
        print(u'{} => {}'.format(doc.id, doc.to_dict()))

# perform a query
key = "dpm-data-integrations-intacct-config"

ref = db.collection(key)

query = ref.where("value","==","Michael Standifer")
docs = query.stream()

for doc in docs:
    print(u'{} => {}'.format(doc.id, doc.to_dict()))
