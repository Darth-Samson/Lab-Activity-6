from pymongo import MongoClient

# --- 1. Connect to MongoDB ---
client = MongoClient("mongodb://localhost:27017/")
db = client["chinook_music"]
temp_artists_collection = db["temp_artists"]
temp_albums_collection = db["temp_albums"]
temp_tracks_collection = db["temp_tracks"]

# The final destination collection we want to build
final_artists_collection = db["artists"]

# Make sure the final collection is empty before we start
final_artists_collection.delete_many({})
print("Cleaned out the final 'artists' collection.")

# --- 3. The Main Logic: Loop and Embed ---

print("Starting to build the nested artist documents...")

# Get all artists from the temporary collection
all_artists = temp_artists_collection.find()

# Loop through every single artist
for artist in all_artists:
    
    new_artist_document = {
        "ArtistId": artist["ArtistId"],
        "Name": artist["Name"],
        "albums": []
    }
    
    artist_albums = temp_albums_collection.find({"ArtistId": artist["ArtistId"]})
    
    for album in artist_albums:
        new_album_object = {
            "AlbumId": album["AlbumId"],
            "Title": album["Title"],
            "tracks": []
        }
        album_tracks = temp_tracks_collection.find({"AlbumId": album["AlbumId"]})
        
        for track in album_tracks:
            new_track_object = {
                "TrackId": track["TrackId"],
                "Name": track["Name"],
                "Composer": track.get("Composer", "N/A"),
                "Milliseconds": track["Milliseconds"],
                "UnitPrice": track["UnitPrice"]
            }
            new_album_object["tracks"].append(new_track_object)

        new_artist_document["albums"].append(new_album_object)
        
    final_artists_collection.insert_one(new_artist_document)
    print(f"Successfully created document for artist: {artist['Name']}")

print("\nAll done! Your 'artists' collection is ready.")
client.close()