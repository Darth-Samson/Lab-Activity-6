from pymongo import MongoClient
import pprint

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Get reference to 'chinook' database
db = client["chinook"]

# --- COLLECTION REFERENCES ---
artists = db["artists"]
albums = db["albums"]
tracks = db["tracks"]

# --- JOIN ARTISTS with ALBUMS ---
print("=== Artists and their Albums ===\n")
artist_album = albums.aggregate([
    {
        "$lookup": {
            "from": "artists",
            "localField": "ArtistId",
            "foreignField": "ArtistId",
            "as": "artist_info"
        }
    },
    {
        "$unwind": "$artist_info"
    },
    {
        "$project": {
            "_id": 0,
            "Album": "$Title",
            "Artist": "$artist_info.Name"
        }
    }
])

for doc in artist_album:
    pprint.pprint(doc)
print("\n")

# --- JOIN ALBUMS with TRACKS ---
print("=== Albums and their Tracks ===\n")
album_tracks = tracks.aggregate([
    {
        "$lookup": {
            "from": "albums",
            "localField": "AlbumId",
            "foreignField": "AlbumId",
            "as": "album_info"
        }
    },
    {
        "$unwind": "$album_info"
    },
    {
        "$project": {
            "_id": 0,
            "TrackName": "$Name",
            "AlbumTitle": "$album_info.Title"
        }
    }
])

for doc in album_tracks:
    pprint.pprint(doc)
print("\n")

# --- FULL JOIN: ARTISTS → ALBUMS → TRACKS ---
print("=== Artists, Albums, and Tracks ===\n")
artist_album_tracks = tracks.aggregate([
    {
        "$lookup": {
            "from": "albums",
            "localField": "AlbumId",
            "foreignField": "AlbumId",
            "as": "album_info"
        }
    },
    {
        "$unwind": "$album_info"
    },
    {
        "$lookup": {
            "from": "artists",
            "localField": "album_info.ArtistId",
            "foreignField": "ArtistId",
            "as": "artist_info"
        }
    },
    {
        "$unwind": "$artist_info"
    },
    {
        "$project": {
            "_id": 0,
            "Artist": "$artist_info.Name",
            "Album": "$album_info.Title",
            "Track": "$Name"
        }
    }
])

for doc in artist_album_tracks:
    pprint.pprint(doc)

client.close()
