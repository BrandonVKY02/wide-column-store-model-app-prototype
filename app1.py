from cassandra.cluster import Cluster

# Function to create a connection to Cassandra running on Docker
def create_cassandra_connection():
    try:
        # Connect to the Cassandra cluster (adjust '127.0.0.1' and port if necessary)
        cluster = Cluster(['127.0.0.1'], port=9042)
        session = cluster.connect()
        print("Connected to Cassandra")
        return session
    except Exception as e:
        print(f"Error connecting to Cassandra: {e}")
        return None

# Function to execute CQL commands
def execute_cql_statements(session, cql_statements):
    try:
        for statement in cql_statements:
            print(f"Executing CQL: {statement}")
            session.execute(statement)
        print("All CQL statements executed successfully")
    except Exception as e:
        print(f"Error executing CQL: {e}")

# List of CQL statements to execute
cql_statements = [
    # Drop and Create Keyspace
    "DROP KEYSPACE IF EXISTS killrvideo;",
    "CREATE KEYSPACE killrvideo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };",
    
    # Use the new keyspace
    "USE killrvideo;",
    
    # Table for user credentials
    "CREATE TABLE user_credentials ("
    "   email text PRIMARY KEY,"
    "   password text,"
    "   userid uuid"
    ");",

    # Table for users
    "CREATE TABLE users ("
    "   userid uuid PRIMARY KEY,"
    "   firstname varchar,"
    "   lastname varchar,"
    "   email text,"
    "   created_date timestamp"
    ");",

    # SASI Indexes for users table
    "CREATE CUSTOM INDEX ON users (firstname) USING 'org.apache.cassandra.index.sasi.SASIIndex' "
    "WITH OPTIONS = {'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer', 'case_sensitive': 'false'};",
    "CREATE CUSTOM INDEX ON users (lastname) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS'};",
    "CREATE CUSTOM INDEX ON users (email) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS'};",
    "CREATE CUSTOM INDEX ON users (created_date) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'SPARSE'};",

    # Create a UDT for video metadata
    "CREATE TYPE video_metadata ("
    "   height int,"
    "   width int,"
    "   video_bit_rate set<text>,"
    "   encoding text"
    ");",

    # Videos table
    "CREATE TABLE videos ("
    "   videoid uuid PRIMARY KEY,"
    "   userid uuid,"
    "   name varchar,"
    "   description varchar,"
    "   location text,"
    "   location_type int,"
    "   preview_thumbnails map<text,text>,"
    "   tags set<varchar>,"
    "   metadata set<frozen<video_metadata>>,"
    "   added_date timestamp"
    ");",

    # Index on tags
    "CREATE INDEX tags_idx ON videos(tags);",

    # Materialized view for videos by location
    "CREATE MATERIALIZED VIEW videos_by_location AS "
    "SELECT userid, added_date, videoid, location "
    "FROM videos WHERE videoid IS NOT NULL AND location IS NOT NULL "
    "PRIMARY KEY(location, videoid);",

    # Table for user videos
    "CREATE TABLE user_videos ("
    "   userid uuid,"
    "   added_date timestamp,"
    "   videoid uuid,"
    "   name text,"
    "   preview_image_location text,"
    "   PRIMARY KEY (userid, added_date, videoid)"
    ") WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC);",

    # Table for latest videos
    "CREATE TABLE latest_videos ("
    "   yyyymmdd text,"
    "   added_date timestamp,"
    "   videoid uuid,"
    "   name text,"
    "   preview_image_location text,"
    "   PRIMARY KEY (yyyymmdd, added_date, videoid)"
    ") WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC);",

    # Table for video ratings
    "CREATE TABLE video_rating ("
    "   videoid uuid PRIMARY KEY,"
    "   rating_counter counter,"
    "   rating_total counter"
    ");",

    # User-defined function to compute average rating
    "CREATE OR REPLACE FUNCTION avg_rating (rating_counter counter, rating_total counter) "
    "CALLED ON NULL INPUT RETURNS double LANGUAGE java AS "
    "'return Double.valueOf(rating_total.doubleValue()/rating_counter.doubleValue());';",

    # Table for user video ratings
    "CREATE TABLE video_ratings_by_user ("
    "   videoid uuid,"
    "   userid uuid,"
    "   rating int,"
    "   PRIMARY KEY (videoid, userid)"
    ");",

    # Table for videos by tag 
    "CREATE TABLE videos_by_tag ("
    "   tag text,"
    "   videoid uuid,"
    "   added_date timestamp,"
    "   name text,"
    "   preview_image_location text,"
    "   tagged_date timestamp,"
    "   PRIMARY KEY (tag, videoid)"
    ");",

    # Table for tags by letter
    "CREATE TABLE tags_by_letter ("
    "   first_letter text,"
    "   tag text,"
    "   PRIMARY KEY (first_letter, tag)"
    ");",
    
    # Table for comments by video
    "CREATE TABLE comments_by_video ("
    "   videoid uuid,"
    "   commentid timeuuid,"
    "   userid uuid,"
    "   comment text,"
    "   PRIMARY KEY (videoid, commentid)"
    ") WITH CLUSTERING ORDER BY (commentid DESC);",

    # Table for comments by user
    "CREATE TABLE comments_by_user ("
    "   userid uuid,"
    "   commentid timeuuid,"
    "   videoid uuid,"
    "   comment text,"
    "   PRIMARY KEY (userid, commentid)"
    ") WITH CLUSTERING ORDER BY (commentid DESC);",

    # Time series for video events
    "CREATE TABLE video_event ("
    "   videoid uuid,"
    "   userid uuid,"
    "   preview_image_location text static,"
    "   event varchar,"
    "   event_timestamp timeuuid,"
    "   video_timestamp bigint,"
    "   PRIMARY KEY ((videoid,userid),event_timestamp,event)"
    ") WITH CLUSTERING ORDER BY (event_timestamp DESC,event ASC);",

    # Table for uploaded videos
    "CREATE TABLE uploaded_videos ("
    "   videoid uuid PRIMARY KEY,"
    "   userid uuid,"
    "   name text,"
    "   description text,"
    "   tags set<text>,"
    "   added_date timestamp,"
    "   jobid text"
    ");",

    # Table for uploaded videos by job ID
    "CREATE TABLE uploaded_videos_by_jobid ("
    "   jobid text PRIMARY KEY,"
    "   videoid uuid,"
    "   userid uuid,"
    "   name text,"
    "   description text,"
    "   tags set<text>,"
    "   added_date timestamp"
    ");",

    # Table for encoding job notifications
    "CREATE TABLE encoding_job_notifications ("
    "   jobid text,"
    "   status_date timestamp,"
    "   etag text,"
    "   newstate text,"
    "   oldstate text,"
    "   PRIMARY KEY (jobid, status_date, etag)"
    ") WITH CLUSTERING ORDER BY (status_date DESC, etag ASC);"
]

# Function to execute CQL commands
def execute_cql_insert_statements(session, cql_insert_statements):
    try:
        for statement in cql_insert_statements:
            print(f"Executing CQL: {statement}")
            session.execute(statement)
        print("All CQL statements executed successfully")
    except Exception as e:
        print(f"Error executing CQL: {e}")

# List of CQL statements to execute
cql_insert_statements = [
    # User_credentials
    "INSERT INTO user_credentials (userid,  email, password)"
    "VALUES (d0f60aa8-54a9-4840-b70c-fe562b68842b,'tcodd@relational.com','5f4dcc3b5aa765d61d8327deb882cf99');",

    "INSERT INTO user_credentials (userid,  email, password)"
    "VALUES (522b1fe2-2e36-4cef-a667-cd4237d08b89,'cdate@relational.com','6cb75f652a9b52798eb6cf2201057c73');",

    "INSERT INTO user_credentials (userid,  email, password)"
    "VALUES (9761d3d7-7fbd-4269-9988-6cfd4e188678,'patrick@datastax.com','ba27e03fd95e507daf2937c937d499ab');",

    # Users
    "INSERT INTO users (userid, firstname, lastname, email, created_date)"
    "VALUES (d0f60aa8-54a9-4840-b70c-fe562b68842b,'Ted','Codd', 'tcodd@relational.com','2011-06-01 08:00:00');",

    "INSERT INTO users (userid, firstname, lastname, email, created_date)"
    "VALUES (522b1fe2-2e36-4cef-a667-cd4237d08b89,'Chris','Date', 'cdate@relational.com','2011-06-20 13:50:00');",

    "INSERT INTO users (userid, firstname, lastname, email, created_date)"
    "VALUES (9761d3d7-7fbd-4269-9988-6cfd4e188678,'Patrick','McFadin', 'patrick@datastax.com','2011-06-20 13:50:00');",

    #Videos
    "INSERT INTO videos (videoid, name, userid, description, location, location_type, preview_thumbnails, tags, added_date, metadata)"
    "VALUES (99051fe9-6a9c-46c2-b949-38ef78858dd0,'My funny cat',d0f60aa8-54a9-4840-b70c-fe562b68842b, 'My cat likes to play the piano! So funny.','/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401',1,{'10':'/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401'},{'cats','piano','lol'},'2012-06-01 08:00:00',"
    "   {"
    "       {"
    "          height: 480,"
    "          width:  640,"
    "          encoding: 'MP4',"
    "          video_bit_rate:"
    "             {"
    "                '1000kbs',"
    "                '400kbs'"
    "             }"
    "       }"
    "   }"
    ");",

"INSERT INTO videos (videoid, name, userid, description, location, location_type, preview_thumbnails, tags, added_date, metadata)"
"VALUES (b3a76c6b-7c7f-4af6-964f-803a9283c401,'Now my dog plays piano!',d0f60aa8-54a9-4840-b70c-fe562b68842b, 'My dog learned to play the piano because of the cat.','/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401',1,{'10':'/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401'},{'dogs','piano','lol'},'2012-08-30 16:50:00',"
"   {"
"       {"
"          height: 480,"
"          width:  640,"
"          encoding: 'MP4',"
"          video_bit_rate:"
"             {"
"                '1000kbs',"
"                '400kbs'"
"             }"
"       }"
"   }"
");",

"INSERT INTO videos (videoid, name, userid, description, location, location_type, preview_thumbnails, tags, added_date, metadata)"
"VALUES (0c3f7e87-f6b6-41d2-9668-2b64d117102c,'An Introduction to Database Systems',522b1fe2-2e36-4cef-a667-cd4237d08b89, 'An overview of my book','/us/vid/0c/0c3f7e87-f6b6-41d2-9668-2b64d117102c',1,{'10':'/us/vid/0c/0c3f7e87-f6b6-41d2-9668-2b64d117102c'},{'database','relational','book'},'2012-09-03 10:30:00',"
"   {"
"       {"
"          height: 480,"
"          width:  640,"
"          encoding: 'MP4',"
"          video_bit_rate:"
"             {"
"                '1000kbs',"
"                '400kbs'"
"             }"
"       }"
"   }"
");",

"INSERT INTO videos (videoid, name, userid, description, location, location_type, preview_thumbnails, tags, added_date, metadata)"
"VALUES (416a5ddc-00a5-49ed-adde-d99da9a27c0c,'Intro to CAP theorem',522b1fe2-2e36-4cef-a667-cd4237d08b89, 'I think there might be something to this.','/us/vid/41/416a5ddc-00a5-49ed-adde-d99da9a27c0c',1,{'10':'/us/vid/41/416a5ddc-00a5-49ed-adde-d99da9a27c0c'},{'database','cap','brewer'},'2012-12-01 11:29:00',"
"   {"
"       {"
"          height: 480,"
"          width:  640,"
"          encoding: 'MP4',"
"          video_bit_rate:"
"             {"
"                '1000kbs',"
"                '400kbs'"
"             }"
"       }"
"   }"
");",

"INSERT INTO videos (videoid, name, userid, description, location, location_type, preview_thumbnails, tags, added_date, metadata)"
"VALUES (06049cbb-dfed-421f-b889-5f649a0de1ed,'The data model is dead. Long live the data model.',9761d3d7-7fbd-4269-9988-6cfd4e188678, 'First in a three part series for Cassandra Data Modeling','http://www.youtube.com/watch?v=px6U2n74q3g',1,{'YouTube':'http://www.youtube.com/watch?v=px6U2n74q3g'},{'cassandra','data model','relational','instruction'},'2013-05-02 12:30:29',"
"   {"
"       {"
"          height: 480,"
"          width:  640,"
"          encoding: 'MP4',"
"          video_bit_rate:"
"             {"
"                '1000kbs',"
"                '400kbs'"
"             }"
"       }"
"   }"
");",

"INSERT INTO videos (videoid, name, userid, description, location, location_type, preview_thumbnails, tags, added_date, metadata)"
"VALUES (873ff430-9c23-4e60-be5f-278ea2bb21bd,'Become a Super Modeler',9761d3d7-7fbd-4269-9988-6cfd4e188678, 'Second in a three part series for Cassandra Data Modeling','http://www.youtube.com/watch?v=qphhxujn5Es',1,{'YouTube':'http://www.youtube.com/watch?v=qphhxujn5Es'},{'cassandra','data model','cql','instruction'},'2013-05-16 16:50:00',"
"   {"
"       {"
"          height: 480,"
"          width:  640,"
"          encoding: 'MP4',"
"          video_bit_rate:"
"             {"
"                '1000kbs',"
"                '400kbs'"
"             }"
"       }"
"   }"
");",

"INSERT INTO videos (videoid, name, userid, description, location, location_type, preview_thumbnails, tags, added_date, metadata)"
"VALUES (49f64d40-7d89-4890-b910-dbf923563a33,'The World''s Next Top Data Model',9761d3d7-7fbd-4269-9988-6cfd4e188678, 'Third in a three part series for Cassandra Data Modeling','http://www.youtube.com/watch?v=HdJlsOZVGwM',1,{'YouTube':'http://www.youtube.com/watch?v=HdJlsOZVGwM'},{'cassandra','data model','examples','instruction'},'2013-06-11 11:00:00',"
"   {"
"       {"
"          height: 480,"
"          width:  640,"
"          encoding: 'MP4',"
"          video_bit_rate:"
"             {"
"                '1000kbs',"
"                '400kbs'"
"             }"
"       }"
"   }"
");",

# user_videos - Every video a user uploads is indexed into a single partition by username
"INSERT INTO user_videos (userid, videoid, added_date, name, preview_image_location)"
"VALUES (d0f60aa8-54a9-4840-b70c-fe562b68842b,99051fe9-6a9c-46c2-b949-38ef78858dd0,'2012-06-01 08:00:00','My funny cat',"
"        '/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401');",

"INSERT INTO user_videos (userid, videoid, added_date, name, preview_image_location)"
"VALUES (d0f60aa8-54a9-4840-b70c-fe562b68842b,b3a76c6b-7c7f-4af6-964f-803a9283c401,'2012-08-30 16:50:00','Now my dog plays piano!',"
"        '/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401');",

"INSERT INTO user_videos (userid, videoid, added_date, name, preview_image_location)"
"VALUES (522b1fe2-2e36-4cef-a667-cd4237d08b89,0c3f7e87-f6b6-41d2-9668-2b64d117102c,'2013-05-02 12:30:29','An Introduction to Database Systems',"
"        '/us/vid/0c/0c3f7e87-f6b6-41d2-9668-2b64d117102c');",

"INSERT INTO user_videos (userid, videoid, added_date, name, preview_image_location)"
"VALUES (522b1fe2-2e36-4cef-a667-cd4237d08b89,416a5ddc-00a5-49ed-adde-d99da9a27c0c,'2012-12-01 11:29:00','Intro to CAP theorem',"
"        '/us/vid/41/416a5ddc-00a5-49ed-adde-d99da9a27c0c');",

"INSERT INTO user_videos (userid, videoid, added_date, name, preview_image_location)"
"VALUES (9761d3d7-7fbd-4269-9988-6cfd4e188678,06049cbb-dfed-421f-b889-5f649a0de1ed,'2013-05-02 12:30:29','The data model is dead. Long live the data model.',"
"        'http://www.youtube.com/watch?v=px6U2n74q3g');",

"INSERT INTO user_videos (userid, videoid, added_date, name, preview_image_location)"
"VALUES (9761d3d7-7fbd-4269-9988-6cfd4e188678,873ff430-9c23-4e60-be5f-278ea2bb21bd,'2013-05-16 16:50:00','Become a Super Modeler',"
"        'http://www.youtube.com/watch?v=qphhxujn5Es');",

"INSERT INTO user_videos (userid, videoid, added_date, name, preview_image_location)"
"VALUES (9761d3d7-7fbd-4269-9988-6cfd4e188678,49f64d40-7d89-4890-b910-dbf923563a33,'2013-06-11 11:00:00','The World''s Next Top Data Model',"
"        'http://www.youtube.com/watch?v=HdJlsOZVGwM');",

# latest_videos
"INSERT INTO latest_videos (yyyymmdd, videoid, added_date, name, preview_image_location)"
"VALUES ('2012-06-01',99051fe9-6a9c-46c2-b949-38ef78858dd0,'2012-06-01 08:00:00','My funny cat',"
"        '/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401');",

"INSERT INTO latest_videos (yyyymmdd, videoid, added_date, name, preview_image_location)"
"VALUES ('2012-08-30',b3a76c6b-7c7f-4af6-964f-803a9283c401,'2012-08-30 16:50:00','Now my dog plays piano!',"
"        '/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401');",

"INSERT INTO latest_videos (yyyymmdd, videoid, added_date, name, preview_image_location)"
"VALUES ('2013-05-02',0c3f7e87-f6b6-41d2-9668-2b64d117102c,'2013-05-02 12:30:29','An Introduction to Database Systems',"
"        '/us/vid/0c/0c3f7e87-f6b6-41d2-9668-2b64d117102c');",

"INSERT INTO latest_videos (yyyymmdd, videoid, added_date, name, preview_image_location)"
"VALUES ('2012-12-01',416a5ddc-00a5-49ed-adde-d99da9a27c0c,'2012-12-01 11:29:00','Intro to CAP theorem',"
"        '/us/vid/41/416a5ddc-00a5-49ed-adde-d99da9a27c0c');",

"INSERT INTO latest_videos (yyyymmdd, videoid, added_date, name, preview_image_location)"
"VALUES ('2013-05-02',06049cbb-dfed-421f-b889-5f649a0de1ed,'2013-05-02 12:30:29','The data model is dead. Long live the data model.',"
"        'http://www.youtube.com/watch?v=px6U2n74q3g');",

"INSERT INTO latest_videos (yyyymmdd, videoid, added_date, name, preview_image_location)"
"VALUES ('2013-05-16',873ff430-9c23-4e60-be5f-278ea2bb21bd,'2013-05-16 16:50:00','Become a Super Modeler',"
"        'http://www.youtube.com/watch?v=qphhxujn5Es');",

"INSERT INTO latest_videos (yyyymmdd, videoid, added_date, name, preview_image_location)"
"VALUES ('2013-06-11',49f64d40-7d89-4890-b910-dbf923563a33,'2013-06-11 11:00:00','The World''s Next Top Data Model',"
"        'http://www.youtube.com/watch?v=HdJlsOZVGwM');",

# Video Rating counters
"UPDATE video_rating SET rating_counter = rating_counter + 1, rating_total = rating_total + 3 "
"WHERE videoid = 99051fe9-6a9c-46c2-b949-38ef78858dd0;",

"UPDATE video_rating SET rating_counter = rating_counter + 1, rating_total = rating_total + 5 "
"WHERE videoid = 99051fe9-6a9c-46c2-b949-38ef78858dd0;",

"UPDATE video_rating SET rating_counter = rating_counter + 1, rating_total = rating_total + 4 "
"WHERE videoid = 99051fe9-6a9c-46c2-b949-38ef78858dd0;",

# video_ratings_by_user
"INSERT INTO video_ratings_by_user (videoid, userid, rating)"
"VALUES ( 99051fe9-6a9c-46c2-b949-38ef78858dd0,9761d3d7-7fbd-4269-9988-6cfd4e188678 ,3);",

"INSERT INTO video_ratings_by_user (videoid, userid, rating)"
"VALUES ( 99051fe9-6a9c-46c2-b949-38ef78858dd0,9761d3d7-7fbd-4269-9988-6cfd4e188678 ,5);",

"INSERT INTO video_ratings_by_user (videoid, userid, rating)"
"VALUES ( 99051fe9-6a9c-46c2-b949-38ef78858dd0,9761d3d7-7fbd-4269-9988-6cfd4e188678 ,4);",

# videos_by_tag
"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('cats',99051fe9-6a9c-46c2-b949-38ef78858dd0,'2012-05-25 08:30:29','2012-06-01 08:00:00','My funny cat',"
"        '/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('piano',99051fe9-6a9c-46c2-b949-38ef78858dd0, '2012-05-25 08:30:29','2012-06-01 08:00:00','My funny cat',"
"        '/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('lol',99051fe9-6a9c-46c2-b949-38ef78858dd0, '2012-05-25 08:30:29','2012-06-01 08:00:00','My funny cat',"
"        '/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('dogs',b3a76c6b-7c7f-4af6-964f-803a9283c401, '2012-08-30 16:50:00','2012-08-30 16:50:00','Now my dog plays piano!',"
"        '/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('piano',b3a76c6b-7c7f-4af6-964f-803a9283c401, '2012-08-30 16:50:00','2012-08-30 16:50:00','Now my dog plays piano!',"
"        '/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('lol',b3a76c6b-7c7f-4af6-964f-803a9283c401, '2012-08-30 16:50:00','2012-08-30 16:50:00','Now my dog plays piano!',"
"        '/us/vid/b3/b3a76c6b-7c7f-4af6-964f-803a9283c401');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('database',0c3f7e87-f6b6-41d2-9668-2b64d117102c, '2012-09-03 10:30:00','2013-05-02 12:30:29','An Introduction to Database Systems',"
"        '/us/vid/0c/0c3f7e87-f6b6-41d2-9668-2b64d117102c');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('relational',0c3f7e87-f6b6-41d2-9668-2b64d117102c, '2012-09-03 10:30:00','2013-05-02 12:30:29','An Introduction to Database Systems',"
"        '/us/vid/0c/0c3f7e87-f6b6-41d2-9668-2b64d117102c');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('book',0c3f7e87-f6b6-41d2-9668-2b64d117102c, '2012-09-03 10:30:00','2013-05-02 12:30:29','An Introduction to Database Systems',"
"        '/us/vid/0c/0c3f7e87-f6b6-41d2-9668-2b64d117102c');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('database',416a5ddc-00a5-49ed-adde-d99da9a27c0c, '2012-12-01 11:29:00','2012-12-01 11:29:00','Intro to CAP theorem',"
"        '/us/vid/41/416a5ddc-00a5-49ed-adde-d99da9a27c0c');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('cap',416a5ddc-00a5-49ed-adde-d99da9a27c0c, '2012-12-01 11:29:00','2012-12-01 11:29:00','Intro to CAP theorem',"
"        '/us/vid/41/416a5ddc-00a5-49ed-adde-d99da9a27c0c');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('brewer',416a5ddc-00a5-49ed-adde-d99da9a27c0c, '2012-12-01 11:29:00','2012-12-01 11:29:00','Intro to CAP theorem',"
"        '/us/vid/41/416a5ddc-00a5-49ed-adde-d99da9a27c0c');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('cassandra',06049cbb-dfed-421f-b889-5f649a0de1ed, '2013-05-02 12:30:29','2013-05-02 12:30:29','The data model is dead. Long live the data model.',"
"        'http://www.youtube.com/watch?v=px6U2n74q3g');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('data model',06049cbb-dfed-421f-b889-5f649a0de1ed, '2013-05-02 12:30:29','2013-05-02 12:30:29','The data model is dead. Long live the data model.',"
"        'http://www.youtube.com/watch?v=px6U2n74q3g');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('relational',06049cbb-dfed-421f-b889-5f649a0de1ed, '2013-05-02 12:30:29','2013-05-02 12:30:29','The data model is dead. Long live the data model.',"
"        'http://www.youtube.com/watch?v=px6U2n74q3g');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('instruction',06049cbb-dfed-421f-b889-5f649a0de1ed, '2013-05-02 12:30:29','2013-05-02 12:30:29','The data model is dead. Long live the data model.',"
"        'http://www.youtube.com/watch?v=px6U2n74q3g');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('cassandra',873ff430-9c23-4e60-be5f-278ea2bb21bd, '2013-05-16 16:50:00','2013-05-16 16:50:00','Become a Super Modeler',"
"        'http://www.youtube.com/watch?v=qphhxujn5Es');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('data model',873ff430-9c23-4e60-be5f-278ea2bb21bd, '2013-05-16 16:50:00','2013-05-16 16:50:00','Become a Super Modeler',"
"        'http://www.youtube.com/watch?v=qphhxujn5Es');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('relational',873ff430-9c23-4e60-be5f-278ea2bb21bd, '2013-05-16 16:50:00','2013-05-16 16:50:00','Become a Super Modeler',"
"        'http://www.youtube.com/watch?v=qphhxujn5Es');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('instruction',873ff430-9c23-4e60-be5f-278ea2bb21bd, '2013-05-16 16:50:00','2013-05-16 16:50:00','Become a Super Modeler',"
"        'http://www.youtube.com/watch?v=qphhxujn5Es');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('cassandra',49f64d40-7d89-4890-b910-dbf923563a33, '2013-06-11 11:00:00','2013-06-11 11:00:00','The World''s Next Top Data Model',"
"        'http://www.youtube.com/watch?v=HdJlsOZVGwM');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('data model',49f64d40-7d89-4890-b910-dbf923563a33, '2013-06-11 11:00:00','2013-06-11 11:00:00','The World''s Next Top Data Model',"
"        'http://www.youtube.com/watch?v=HdJlsOZVGwM');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('examples',49f64d40-7d89-4890-b910-dbf923563a33, '2013-06-11 11:00:00','2013-06-11 11:00:00','The World''s Next Top Data Model',"
"        'http://www.youtube.com/watch?v=HdJlsOZVGwM');",

"INSERT INTO videos_by_tag (tag, videoid, tagged_date, added_date, name, preview_image_location)"
"VALUES ('instruction',49f64d40-7d89-4890-b910-dbf923563a33, '2013-06-11 11:00:00','2013-06-11 11:00:00','The World''s Next Top Data Model',"
"        'http://www.youtube.com/watch?v=HdJlsOZVGwM');",

# Video Comments. One for each side of the view.
# Insert in pairs
# This is done using the logged batch command to group our operations to ensure both actions are eventually taken.
"BEGIN BATCH"
"   INSERT INTO comments_by_video (videoid, userid, commentid, comment)"
"   VALUES (99051fe9-6a9c-46c2-b949-38ef78858dd0,d0f60aa8-54a9-4840-b70c-fe562b68842b,now(), 'Worst. Video. Ever.');"
"   INSERT INTO comments_by_video (videoid, userid, commentid, comment)"
"   VALUES (99051fe9-6a9c-46c2-b949-38ef78858dd0,d0f60aa8-54a9-4840-b70c-fe562b68842b,now(), 'Worst. Video. Ever.');"
"APPLY BATCH;",

"BEGIN BATCH"
"   INSERT INTO comments_by_video (videoid, userid, commentid, comment)"
"   VALUES (99051fe9-6a9c-46c2-b949-38ef78858dd0,522b1fe2-2e36-4cef-a667-cd4237d08b89,now(), 'It is amazing');"
"   INSERT INTO comments_by_video (videoid, userid, commentid, comment)"
"   VALUES (99051fe9-6a9c-46c2-b949-38ef78858dd0,522b1fe2-2e36-4cef-a667-cd4237d08b89,now(), 'It is amazing');"
"APPLY BATCH;",

# Video events
"INSERT INTO video_event (videoid, userid, event, event_timestamp, video_timestamp)"
"VALUES (99051fe9-6a9c-46c2-b949-38ef78858dd0,d0f60aa8-54a9-4840-b70c-fe562b68842b,'start',now(),0);",
"INSERT INTO video_event (videoid, userid, event, event_timestamp, video_timestamp)"
"VALUES (99051fe9-6a9c-46c2-b949-38ef78858dd0,d0f60aa8-54a9-4840-b70c-fe562b68842b,'stop',now(),30000);",
"INSERT INTO video_event (videoid, userid, event, event_timestamp, video_timestamp)"
"VALUES (99051fe9-6a9c-46c2-b949-38ef78858dd0,d0f60aa8-54a9-4840-b70c-fe562b68842b,'start',now(),3000);",
"INSERT INTO video_event (videoid, userid, event, event_timestamp, video_timestamp)"
"VALUES (99051fe9-6a9c-46c2-b949-38ef78858dd0,d0f60aa8-54a9-4840-b70c-fe562b68842b,'stop',now(),230000);"
]

# Main function to establish connection and run CQL
if __name__ == "__main__":
    session = create_cassandra_connection()
    if session:
        execute_cql_statements(session, cql_statements)
        execute_cql_insert_statements(session, cql_insert_statements)
        session.shutdown()
