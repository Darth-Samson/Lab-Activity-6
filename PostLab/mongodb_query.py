from pymongo import MongoClient
import pprint
import re

client = MongoClient("mongodb://localhost:27017/")
db = client["chinook"]
customers_collection = db["customers"]


# Query 1: Print the first document found (Page 5)
# doc1 = customers_collection.find_one()
# print(doc1)


# Query 2: Print all documents (Page 5-6)
# for all_doc in customers_collection.find():
#     print(all_doc)


# Query 3: Return only the LastName and FirstName (Page 6)
# for rec in customers_collection.find({}, {"_id": 0, "LastName": 1, "FirstName": 1}):
#     print(rec)


# Query 4: Print all customers with LastName starting with "G" (Page 7)
# rgx = re.compile('^G.*', re.IGNORECASE)
# cursor = customers_collection.find({"LastName": rgx})
# num_docs = 0
# for document in cursor:
#     num_docs += 1
#     pprint.pprint(document)
#     print()
# print("# of documents found: " + str(num_docs))


# Query 5: Print all customers with LastName starting with "Go" (Page 8)
rgx = re.compile('^Go.*', re.IGNORECASE)
cursor = customers_collection.find({"LastName": rgx})
num_docs = 0
for document in cursor:
    num_docs += 1
    pprint.pprint(document)
    print()
print("# of documents found: " + str(num_docs))


client.close()