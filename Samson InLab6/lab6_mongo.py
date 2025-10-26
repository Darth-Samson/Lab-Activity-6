from pymongo import MongoClient
import pprint
import re

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Get reference to 'chinook' database
db = client["chinook"]

# Get a reference to the 'customers' collection
customers_collection = db["customers"]

# Print the first document
print("=== First Document ===")
doc1 = customers_collection.find_one()
pprint.pprint(doc1)

# Print all documents
print("\n=== All Documents ===")
for all_doc in customers_collection.find():
    pprint.pprint(all_doc)

# Print only the LastName and FirstName
print("\n=== Only Names ===")
for rec in customers_collection.find({}, {"_id": 0, "LastName": 1, "FirstName": 1}):
    pprint.pprint(rec)

# Print all customers with LastName starting with 'G'
print("\n=== Customers with LastName starting with 'G' ===")
rgx = re.compile('^G.*?$', re.IGNORECASE)
cursor = customers_collection.find({"LastName": rgx})
num_docs = 0
for document in cursor:
    num_docs += 1
    pprint.pprint(document)
    print()

print("# of documents found:", num_docs)

client.close()
