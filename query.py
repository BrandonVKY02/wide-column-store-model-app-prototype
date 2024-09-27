from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])  # Use the IP address of your Docker container if needed
session = cluster.connect()

# Use the Keyspace
session.set_keyspace('killrvideo')

# First Query
statement_1 = 'SELECT * FROM users;'
rows_1 = session.execute(statement_1)
print("Query 1\nName\t\tEmail")
for row in rows_1:
    print(f"{row.firstname} {row.lastname}\t{row.email}")

# Second Query
statement_2 = 'SELECT firstname, lastname FROM users WHERE userid = d0f60aa8-54a9-4840-b70c-fe562b68842b;'
rows_2 = session.execute(statement_2)
print("\nQuery 2\nName")
for row in rows_2:
    print(f"{row.firstname} {row.lastname}")

# Third Query
statement_3 = 'SELECT * FROM videos WHERE videoId = 06049cbb-dfed-421f-b889-5f649a0de1ed;'
print("\nQuery 3")
rows_3 = session.execute(statement_3)
for row in rows_3:
    print(f"Video Id: {row.videoid}")
    print(f"Description: {row.description}")
    print(f"Location: {row.location}")
    print(f"Location Type: {row.location_type}")
    print(f"Name: {row.name}")
    print(f"User Id: {row.userid}")
    print(f"Metadata: {row.metadata}")
    print(f"Preview Thumbnails: {row.preview_thumbnails}")
    print(f"Tag: {row.tags}")

# Fourth Query
statement_4 = 'SELECT tags FROM videos WHERE videoid = 06049cbb-dfed-421f-b889-5f649a0de1ed;'
print("\nQuery 4")
rows_4 = session.execute(statement_4)
for row in rows_4:
    print(f"Tag: {row.tags}")

# Fifth Query
statement_5 = 'SELECT location FROM videos WHERE videoid = 06049cbb-dfed-421f-b889-5f649a0de1ed;'
print("\nQuery 5")
rows_5 = session.execute(statement_5)
for row in rows_5:
    print(f"Location: {row.location}")

# Sixth Query
statement_6 = 'SELECT name,videoID,added_date FROM user_videos WHERE userid = 522b1fe2-2e36-4cef-a667-cd4237d08b89;'
print("\nQuery 6\n")

# Define column headers
headers = ["Video Name", "Video ID", "Add Date"]

rows_6 = session.execute(statement_6)

data_6 = [headers]
for row in rows_6:
    data_6.append([row.name, row.videoid, row.added_date])

# Determine column widths
col_widths = [max(len(str(item)) for item in col) + 2 for col in zip(*data_6)]

# Print the table header with proper alignment
print("".join(str(item).ljust(width) for item, width in zip(headers, col_widths)))
print("-" * sum(col_widths))

# Print the rows with aligned columns
for row in data_6[1:]:  # Skip headers already printed
    print("".join(str(item).ljust(width) for item, width in zip(row, col_widths)))

# Seventh Query
statement_7 = 'SELECT name,videoID,added_date FROM user_videos WHERE userid = 9761d3d7-7fbd-4269-9988-6cfd4e188678 ORDER BY added_date DESC;'
print("\nQuery 7\n")

rows_7 = session.execute(statement_7)

data_7 = [headers]
for row in rows_7:
    data_7.append([row.name, row.videoid, row.added_date])

# Determine column widths
col_widths = [max(len(str(item)) for item in col) + 2 for col in zip(*data_7)]

# Print the table header with proper alignment
print("".join(str(item).ljust(width) for item, width in zip(headers, col_widths)))
print("-" * sum(col_widths))

# Print the rows with aligned columns
for row in data_7[1:]:  # Skip headers already printed
    print("".join(str(item).ljust(width) for item, width in zip(row, col_widths)))
    
# Eigth Query
statement_8 = """
SELECT name, videoID, added_date 
FROM user_videos 
WHERE userid = 9761d3d7-7fbd-4269-9988-6cfd4e188678 
  AND added_date > '2013-05-15' 
  AND added_date < '2013-07-01'
ORDER BY added_date ASC;
"""
print("\nQuery 8\n")

rows_8 = session.execute(statement_8)

data_8 = [headers]
for row in rows_8:
    data_8.append([row.name, row.videoid, row.added_date])

# Determine column widths
col_widths = [max(len(str(item)) for item in col) + 2 for col in zip(*data_8)]

# Print the table header with proper alignment
print("".join(str(item).ljust(width) for item, width in zip(headers, col_widths)))
print("-" * sum(col_widths))

# Print the rows with aligned columns
for row in data_8[1:]:  # Skip headers already printed
    print("".join(str(item).ljust(width) for item, width in zip(row, col_widths)))
    
# Ninth Query
statement_9 = 'SELECT rating_counter, rating_total FROM video_rating WHERE videoId = 99051fe9-6a9c-46c2-b949-38ef78858dd0;'
rows_9 = session.execute(statement_9)
print("\nQuery 9\nRating Counter\tRating Total")
for row in rows_9:
    print(f"{row.rating_counter}\t\t{row.rating_total}")

# Tenth Query
statement_10 = "SELECT videoID, tagged_date FROM videos_by_tag WHERE tag = 'lol';"
rows_10 = session.execute(statement_10)
print("\nQuery 10\nVideo ID\t\t\t\tTag Date")
for row in rows_10:
    print(f"{row.videoid}\t{row.tagged_date}")      

# Eleven Query
statement_11 = 'SELECT userid, comment, dateOf(commentid) FROM comments_by_video WHERE videoid = 99051fe9-6a9c-46c2-b949-38ef78858dd0;'
print("\nQuery 11\n")

# Define column headers
headers_2 = ["User Id", "Comment", "Date of Comment"]

rows_11 = session.execute(statement_11)

data_11 = [headers_2]
for row in rows_11:
    data_11.append([row.userid, row.comment, row.system_dateof_commentid])

# Determine column widths
col_widths_2 = [max(len(str(item)) for item in col) + 2 for col in zip(*data_11)]

# Print the table header with proper alignment
print("".join(str(item).ljust(width) for item, width in zip(headers_2, col_widths_2)))
print("-" * sum(col_widths_2))

# Print the rows with aligned columns
for row in data_11[1:]:  # Skip headers already printed
    print("".join(str(item).ljust(width) for item, width in zip(row, col_widths_2)))

# Twelve Query
statement_12 = "SELECT dateOf(event_timestamp), event, video_timestamp FROM video_event WHERE videoID = 99051fe9-6a9c-46c2-b949-38ef78858dd0 AND userid= d0f60aa8-54a9-4840-b70c-fe562b68842b limit 5;"
rows_12 = session.execute(statement_12)
print("\nQuery 12\nEvent Timestamp\t\t\tEvent\tVideo Timestamp")
for row in rows_12:
    print(f"{row.system_dateof_event_timestamp}\t{row.event}\t{row.video_timestamp}")     

# Close the session
session.shutdown()
