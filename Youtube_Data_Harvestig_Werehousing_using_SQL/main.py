import datetime
import googleapiclient.discovery
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
import mysql.connector
import streamlit as st
from streamlit_option_menu import option_menu


api_key ='**********************************' #(API Key generated from google developer console)
# channel_id = 'UCBwmMxybNva6P_5VmxjzwqA'
youtube = googleapiclient.discovery.build('youtube','v3',developerKey=api_key)


def channel_details(youtube, channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
    response = request.execute()
    Channel_data = dict(Channel_name = response['items'][0]['snippet']['title'],
                Channel_id = response['items'][0]['id'],
                Upload_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                Channel_discription = response['items'][0]['snippet']['description'],
                Subcriber_count = response['items'][0]['statistics']['subscriberCount'],
                Videos_count = response['items'][0]['statistics']['videoCount'],
                Channel_Views = response['items'][0]['statistics']['viewCount'],
                Channel_country = response['items'][0]['snippet'].get('country')
               )
    return Channel_data


def upload_id(youtube, channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
    response = request.execute()
    upload_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    return upload_id



def playlist (youtube, channel_id):
    request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults = 50
    )
    response = request.execute()
    playlist_data=[]
    for i in range(0,len(response['items'])):
        data = dict(
                Channel_name = response['items'][i]['snippet']['channelTitle'],
                Playlist_tilte = response['items'][i]['snippet']['title'],
                Channel_id = response['items'][i]['snippet']['channelId'],
                Playlist_id = response['items'][i]['id'],
                Playlist_discription = response['items'][i]['snippet']['description'],
                Playlist_vidoecount = response['items'][i]['contentDetails']['itemCount']
        )
        playlist.append(data)
    return playlist_data

def videos_ids(youtube,Upload_id):
    request =  youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=Upload_id,
            maxResults = 50
            )
    response = request.execute()
    video_ids =[]
    next_page_token = response.get('nextPageToken')
    more_pages = True
    
    for i in range(len(response['items'])):
        video_id = response['items'][i]['contentDetails']['videoId']
        video_ids.append(video_id)

    while more_pages :
        if next_page_token is None :
            more_pages = False
        else:
            request =  youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=Upload_id,
                maxResults = 50,
                pageToken = next_page_token
            )
            response = request.execute()
            next_page_token = response.get('nextPageToken')

            for i in range(len(response['items'])):
                video_id = response['items'][i]['contentDetails']['videoId']
                video_ids.append(video_id)
            
    return video_ids

       

def all_video_datail(youtube,video_ids):
    videos_data=[]
    for i in range(0,len(video_ids),50):
        request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=','.join(video_ids[i:i+50])
            )
        response = request.execute()
        
        for item in response['items']:
            video_id = item['id']
            video_detail = dict(
                    Channel_name = item['snippet']['channelTitle'],
                    Channel_id = item['snippet']['channelId'],
                    Video_id = item['id'],
                    Title = item['snippet']['title'],
                    # Tags = item['snippet'].get('tags'),
                    Tags = ','.join(map(str,item['snippet'].get('tags'))) if (item['snippet'].get('tags')) is not None else item['snippet'].get('tags'),
                    Thumbnail = item['snippet']['thumbnails']['default']['url'],
                    Video_description = item['snippet']['description'],
                    # Published_date = item['snippet']['publishedAt'],
                    Published_date = pd.Timestamp(item['snippet']['publishedAt']),
                    Duration = int(pd.Timedelta(item['contentDetails']['duration']).total_seconds()),
                    # Duration = item['contentDetails']['duration'],
                    View_count = item['statistics'].get('viewCount'),
                    Like_count = item['statistics'].get('likeCount'),
                    Comment_count = item['statistics'].get('commentCount'),
                    Favorite_count = item['statistics']['favoriteCount'],
                    Definition = item['contentDetails']['definition'],
                    Caption_status = item['contentDetails'].get('caption')
                    )
            videos_data.append(video_detail)
    return videos_data



def comment_details(youtube,video_ids):
    Comments=[]
    vi_count = 0
    for video_id in video_ids :
        if vi_count < 10 :
            request = youtube.commentThreads().list(
                                            part="snippet,replies",
                                            videoId = video_id,
                                            maxResults = 100
                                            )
            response = request.execute()
            for comment in response['items']:
                data = dict(Comment_id = comment['id'],
                            Video_id = comment['snippet']['videoId'],
                            Comment_text = comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = pd.Timestamp(comment['snippet']['topLevelComment']['snippet']['publishedAt']),
                            Like_count = comment['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = comment['snippet']['totalReplyCount']
                           )
                Comments.append(data)
    
            next_page_token = response.get('nextPageToken')
            page_count = 1
            more_pages = True
            vi_count+=1
            
            while more_pages :
                if next_page_token is None or page_count == 2 :
                        more_pages = False
                else:
                    request = youtube.commentThreads().list(
                                                part="snippet,replies",
                                                videoId = video_id,
                                                maxResults = 100,
                                                pageToken = next_page_token
                                                )
                    response = request.execute()
                    for comment in response['items']:
                        data = dict(Comment_id = comment['id'],
                                    Video_id = comment['snippet']['videoId'],
                                    Comment_text = comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                                    Comment_author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                    Comment_posted_date = pd.Timestamp(comment['snippet']['topLevelComment']['snippet']['publishedAt']),
                                    Like_count = comment['snippet']['topLevelComment']['snippet']['likeCount'],
                                    Reply_count = comment['snippet']['totalReplyCount']
                                   )
                        Comments.append(data)
                    next_page_token = response.get('nextPageToken')
                    page_count+=1 
        else:
            break
    return Comments

def data_extraction():
    Channel_data = channel_details(youtube, channel_id)
    Upload_id = upload_id(youtube, channel_id)
    video_ids = videos_ids(youtube,Upload_id)
    Videos = all_video_datail(youtube,video_ids)
    Comments = comment_details(youtube,video_ids)
    Channel_State = [{
    "Channel_info" : Channel_data,
    "Vidoes_info" : Videos,
    "Comments_info" : Comments
    }]
    return Channel_State


### For MongoDB 

def name_collection():
    Channel_names=[]
    connection = MongoClient('mongodb://localhost:27017')
    db = connection['Youtube_Chennal_Data']
    name_col = db['Channel_Names']
    for name in name_col.find({},{'_id':0}):
        Channel_names.append(name.get('Ch_Name'))
    return Channel_names

def mongodb_database():
    connection = MongoClient('mongodb://localhost:27017')
    db = connection['Youtube_Chennal_Data']
    name_col = db[Channel_data['Channel_name']]
    Channel_names = name_collection()

    if Channel_data['Channel_name'] not in Channel_names :
        name_col = db['Channel_Names']
        Channel_State = data_extraction()

        name_col.insert_one({'Ch_Name' : Channel_State[0]["Channel_info"].get("Channel_name")})
        collection_Channel = db[Channel_State[0]["Channel_info"]["Channel_name"]]
        collection_Channel.insert_many(Channel_State)
        return True
        
    else :
        return False
    

def delete_opretion(Name_list):
    connection = MongoClient('mongodb://localhost:27017')
    db = connection['Youtube_Chennal_Data'] 
    for Name in Name_list:
        collection_remove = db['Channel_Names']
        query = {"Ch_Name":Name}
        collection_remove.delete_one(query)
        
        collection_remove = db[Name]
        collection_remove.drop()


def display_data():
    connection = MongoClient('mongodb://localhost:27017')
    db = connection['Youtube_Chennal_Data']
    present_Channels = name_collection()
    display_data=[]
    for name in present_Channels:
        collection_Channel = db[name]
        for row in collection_Channel.find({},{'_id':0}):
            data = dict(
                    Channel_Name = row["Channel_info"]["Channel_name"],
                    Channel_discription = row["Channel_info"]["Channel_discription"]
                    )
            display_data.append(data)
    return display_data


### for SQL 

conn = mysql.connector.connect(
                                host = 'localhost',
                                user = 'root',
                                password = '12345678',
                                database = 'youtube_data'
                                )
cursor = conn.cursor()

def table_creation():
    create_query = '''CREATE TABLE channels (Channel_name VARCHAR (100) ,
                                                            Channel_id VARCHAR (80)primary key,
                                                            Upload_id VARCHAR(80),
                                                            Channel_discription text,
                                                            Subcriber_count int,
                                                            Videos_count int,
                                                            channel_views int(30),
                                                            Channel_country VARCHAR (60))'''       
    cursor.execute(create_query)
    conn.commit()

    create_query = '''CREATE TABLE videos (Channel_name VARCHAR(100),
                                                    Channel_id VARCHAR(100),
                                                    Video_id VARCHAR(100) PRIMARY KEY,
                                                    Title VARCHAR(100),
                                                    Tags TEXT,
                                                    Thumbnail VARCHAR(255),
                                                    Video_description TEXT,
                                                    Published_date TIMESTAMP,
                                                    Duration INT,
                                                    View_count INT,
                                                    Like_count INT,
                                                    Comment_count INT,
                                                    Favorite_count INT,
                                                    Definition VARCHAR(100),
                                                    Caption_status VARCHAR(100))'''
    cursor.execute(create_query)
    conn.commit()

    create_query = '''CREATE TABLE comments (Comment_id VARCHAR (100) primary key,
                                                    Video_id VARCHAR (100),
                                                    Comment_text text,
                                                    Comment_author VARCHAR (100),
                                                    Comment_posted_date timestamp)'''
    cursor.execute(create_query)
    conn.commit()


def channels_table():
    connection = MongoClient('mongodb://localhost:27017')
    db = connection['Youtube_Chennal_Data'] 
    for name in Name_list:
        collection1 = db[name]
        df = pd.DataFrame(collection1.find({},{'_id':0}))
        try:
            create_query = '''CREATE TABLE channels (Channel_name VARCHAR (100) ,
                                                            Channel_id VARCHAR (80)primary key,
                                                            Upload_id VARCHAR(80),
                                                            Channel_discription text,
                                                            Subcriber_count int,
                                                            Videos_count int,
                                                            channel_views int(30),
                                                            Channel_country VARCHAR (60))'''       
            cursor.execute(create_query)
            conn.commit()

        except:
            pass

        
        for row in df.Channel_info:
            insert_query = '''INSERT INTO channels (Channel_name, Channel_id, Upload_id, 
                                                    Channel_discription, Subcriber_count, 
                                                    Videos_count, channel_views, Channel_country) 
                                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''
        
            values =(
                    row['Channel_name'],
                    row['Channel_id'],
                    row['Upload_id'],
                    row['Channel_discription'],
                    row['Subcriber_count'],
                    row['Videos_count'],
                    row['Channel_Views'],
                    row['Channel_country'])
            
            cursor.execute(insert_query,values)
            conn.commit()

    st.write("Channel values are inserted")
            



def videos_table():
    
    connection = MongoClient('mongodb://localhost:27017')
    db = connection['Youtube_Chennal_Data'] 
    for name in Name_list:
        # print(name)
        collection1 = db[name]
        df = pd.DataFrame(collection1.find({},{'_id':0}))

        try:
            create_query = '''CREATE TABLE videos (Channel_name VARCHAR(100),
                                                    Channel_id VARCHAR(100),
                                                    Video_id VARCHAR(100) PRIMARY KEY,
                                                    Title VARCHAR(100),
                                                    Tags TEXT,
                                                    Thumbnail VARCHAR(255),
                                                    Video_description TEXT,
                                                    Published_date TIMESTAMP,
                                                    Duration INT,
                                                    View_count INT,
                                                    Like_count INT,
                                                    Comment_count INT,
                                                    Favorite_count INT,
                                                    Definition VARCHAR(100),
                                                    Caption_status VARCHAR(100))'''
            cursor.execute(create_query)
            conn.commit()
            
        except:
            pass

            # try:
        insert_query = '''INSERT INTO videos (Channel_name, Channel_id, 
                                                Video_id, Title,    
                                                Tags, Thumbnail, Video_description,
                                                Published_date,
                                                Duration, View_count, Like_count,
                                                Comment_count, Favorite_count,
                                                Definition, Caption_status) 
                                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''            
        for row in df.Vidoes_info:
            for i in range(0,len(row)-1):
                values = (
                            row[i]['Channel_name'],
                            row[i]['Channel_id'],
                            row[i]['Video_id'],
                            row[i]['Title'],
                            # ','.join(map(str,(row[i]['Tags']))),
                            row[i]['Tags'],
                            row[i]['Thumbnail'],
                            row[i]['Video_description'],
                            row[i]['Published_date'],
                            row[i]['Duration'],
                            row[i]['View_count'],
                            row[i]['Like_count'],
                            row[i]['Comment_count'],
                            row[i]['Favorite_count'],
                            row[i]['Definition'],
                            row[i]['Caption_status'])
                                    
                cursor.execute(insert_query,values)
                conn.commit()    

                #  except:
                # print(values)

    st.write("Video values are inserted")



def comments_table():
    connection = MongoClient('mongodb://localhost:27017')
    db = connection['Youtube_Chennal_Data'] 
    for name in Name_list:
        # print(name)
        collection1 = db[name]
        df = pd.DataFrame(collection1.find({},{'_id':0}))

    try:
        create_query = '''CREATE TABLE comments (Comment_id VARCHAR (100) primary key,
                                                    Video_id VARCHAR (100),
                                                    Comment_text text,
                                                    Comment_author VARCHAR (100),
                                                    Comment_posted_date timestamp)'''
        cursor.execute(create_query)
        conn.commit()
    except:
        pass

    insert_query = '''INSERT INTO comments (Comment_Id, Video_Id, Comment_Text, 
                                    Comment_Author, Comment_posted_date) 
                                    VALUES (%s, %s, %s, %s, %s)'''

    for row in df.Comments_info:
        for i in range(0,len(row)-1):
            values =(
                    row[i]['Comment_id'],
                    row[i]['Video_id'],
                    row[i]['Comment_text'],
                    row[i]['Comment_author'],
                    row[i]['Comment_posted_date'])
                            
            cursor.execute(insert_query,values)
            conn.commit()    
    st.write("comments values are inserted")


def data_Tables():
    channels_table()
    videos_table()
    comments_table()

def show_channel_table():
    SQL_name = []
    search_qurry = '''SELECT * FROM channels '''
    cursor.execute(search_qurry)
    x = cursor.fetchall()
    for y in x:
        SQL_name.append(y)
    return SQL_name



# page configuration
st.set_page_config(page_title='YouTube Data Harvesting and Warehousing', layout="wide")

# title and position
st.markdown(f'<h1 style="text-align: center;">YouTube Data Harvesting and Warehousing</h1>',
            unsafe_allow_html=True)

with st.sidebar:
    option = option_menu(menu_title='', options=['Data Retrive from YouTube API', 'Manege Data of MongoDB',
                                                 'Migrating Data to SQL', 'SQL Queries'],
                         icons=['youtube', 'database-add', 'database-fill-check', 'pencil-square'])
    

if option == 'Data Retrive from YouTube API':

    col1,col2 = st.columns([3,1])
    with col1:
        st.markdown("#    ")
        st.write("#### Retrive Channel Data From Youtube :")
        channel_id = st.text_input("Enter Channel ID").split(',')
        extract = st.button("Extract Data")
        st.write('UCBwmMxybNva6P_5VmxjzwqA '
                'UC4JX40jDee_tINbkjycV4Sg '
                'UCh9nVJoWXmFb7sLApWGcLPQ ')

        if extract :
            try:
                Channel_data = channel_details(youtube, channel_id)
                st.dataframe(Channel_data)
                
            except :
                st.warning("Please Enter a vailid Channel ID", icon ="⚠️")
                

    with col2:
        try:
            st.markdown("#      ")
            st.write("#### To Store Data to MongoDB :")
            if st.button('Import to MongoDB') :
                with st.spinner('Please wait .... '):
                    Channel_data = channel_details(youtube, channel_id)
                    data_extracted = mongodb_database()
                    if data_extracted == True :
                        st.success(f"The {Channel_data['Channel_name']} Channel data successfully save to MongoDB")
                    else:
                        st.warning(f"The {Channel_data['Channel_name']} Channel data has already in MongoDB database")
        except:
            st.warning(f"Please Extract the Channel Data first! ")


if option == 'Manege Data of MongoDB':
    display_description = display_data()
    names = name_collection()
    def description_op():
        description_data = []
        for i in range (len(names)):
            if display_description[i]['Channel_Name'] in Name_list:
                description_data.append(display_description[i])
        return description_data


    col1,col2 = st.columns([3,1])
    with col1 :
        Name_list = st.multiselect(' Select the Channel you want to Update or Remove',names)
        st.write('You selected:', Name_list)
        st.dataframe(description_op())
    with col2:
        if Name_list !=[] :
            st.write('#### To Delete the Selected Date form Database')
            if st.button('Delete'):
                delete_opretion(Name_list)
                st.success("The Data has been Deleted Form Database")


if option == 'Migrating Data to SQL':
    names = name_collection()
    col1,col2 = st.columns([3,1])
    with col1 :
        SQL_empty = None
        try:
            check_name = []
            search_qurry = '''SELECT Channel_name FROM channels '''
            cursor.execute(search_qurry)
            x = cursor.fetchall()
            for y in x:
                check_name.append(y)
        except:
            SQL_empty = True
        
        st.write("## SQL Channel Data")
        st.subheader("Data of Those Channel Which is Already in SQL")
        if SQL_empty == True:
            st.warning("There is NO Data NOW Present in SQL")
        else :
            df = pd.DataFrame(show_channel_table(),columns=cursor.column_names)
            st.write(df)

    with col2:
        Name_list = st.multiselect(' ### Select the Channel',names)
        st.write('##### To Migrate the Selected Data to SQL')
        migrate =st.button('Migrate')
        st.write('You selected:', pd.DataFrame(Name_list))
        if Name_list !=[] :
            if migrate:
                try:
                    with st.spinner('Please wait .... '):
                        data_Tables()
                        st.success("The Data has been Migrated to SQL")
                except:
                    st.warning("The Selected ONE or MORE Names are Already in SQL")



# VIEW PAGE
                    
if option == 'SQL Queries':
    
    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
    ['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
    
    if questions == '1. What are the names of all the videos and their corresponding channels?':
        cursor.execute("""SELECT title AS Video_Title, channel_name AS Channel_Name FROM videos ORDER BY channel_name""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        cursor.execute("""SELECT channel_name 
        AS Channel_Name, Videos_count AS Total_Videos
                            FROM channels
                            ORDER BY total_videos DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        #st.bar_chart(df,x= cursor.column_names[0],y= cursor.column_names[1])
        fig = px.bar(df,
                     x=cursor.column_names[0],
                     y=cursor.column_names[1],
                     orientation='v',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        cursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, View_count AS Views 
                            FROM videos
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=cursor.column_names[2],
                     y=cursor.column_names[1],
                     orientation='h',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        cursor.execute("""SELECT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments
                            FROM videos AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comments GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
          
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name AS Channel_Name,title AS Title, Like_count AS Likes_Count 
                            FROM videos
                            ORDER BY Likes_Count DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=cursor.column_names[2],
                     y=cursor.column_names[1],
                     orientation='h',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        cursor.execute("""SELECT channel_name AS Channel_Name, title AS Title, Like_count AS Likes_Count
                            FROM videos
                            ORDER BY Like_count DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
         
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name AS Channel_Name, channel_views AS Views
                            FROM channels
                            ORDER BY views DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=cursor.column_names[0],
                     y=cursor.column_names[1],
                     orientation='v',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute("""SELECT channel_name AS Channel_Name
                            FROM videos
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        def time_for():
            tm = [str(datetime.timedelta(seconds=int(i))) for i in df["average_duration"]]
            return tm
        cursor.execute("""SELECT channel_name, 
                        SUM(Duration)/COUNT(*) AS average_duration
                        FROM videos
                        GROUP BY channel_name""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        df["average_duration"] = time_for()

        st.write("### :green[Average video duration for channels :]")
        st.write(df)
        

        
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name AS Channel_Name,Title as Title ,video_id AS Video_ID, Comment_count AS Comments
                            FROM videos
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        st.write("### :green[Videos with most comments :]")
        fig = px.bar(df,
                     x=cursor.column_names[2],
                     y=cursor.column_names[3],
                     orientation='v',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)               
