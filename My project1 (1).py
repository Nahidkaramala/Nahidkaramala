#!/usr/bin/env python
# coding: utf-8

# In[7]:


from pprint import pprint
import googleapiclient.discovery 
from googleapiclient.discovery import build


# In[8]:


api_service_name = "youtube"
api_version = "v3"
api_key='AIzaSyDsuWe5FK6Q8VGTV9iNOExCwDQGis8KhbI'


youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)



# In[9]:


def channel_details(channel_id):
#     all_channel_details=[]
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response_ch = request.execute()
    for i in response_ch['items']:
        ch_data=dict(
        Channel_Name=response_ch['items'][0]['snippet']['title'],
        Channel_Id=response_ch['items'][0]['id'],
        Subscription_Count=response_ch['items'][0]['statistics']['subscriberCount'],
        view_count=response_ch['items'][0]['statistics']['viewCount'],
        Channel_Description=response_ch['items'][0]['snippet']['description'],
        Channel_pAt=response_ch['items'][0]['snippet']['publishedAt'],
        Playlist_Id=response_ch['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
#         all_channel_details.append(ch_data)
    

    return ch_data
   


# In[10]:


ch_d=channel_details('UCABUpiUH2LbLmWu_i2CTFRA')
pprint(ch_d)


# In[11]:


def playlist_id(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response_playlist_id = request.execute()
    
    if 'items' in response_playlist_id and response_playlist_id['items']:
        playlist_id = response_playlist_id['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        return playlist_id   
    else:
        return None



# In[12]:


playlist_id_value = playlist_id('UCABUpiUH2LbLmWu_i2CTFRA')
pprint(playlist_id_value)


# In[13]:


# GETTING VIDEO_ID DETAILS

def get_videos_ids(playlistId):
 
    video_ids = []
    token = None

    while True:
    # Make the request with the playlistId parameter
        request = youtube.playlistItems().list(
        part='snippet',
        maxResults=50,
        pageToken=token,
        playlistId=playlist_id_value)
        response=request.execute()

        for i in response['items']:
            video_ids.append(i['snippet']['resourceId']['videoId'])

    # Check if there are more pages
        if 'nextPageToken' in response:
            token = response.get('nextPageToken')
        else:
            break
    return(video_ids)


# In[14]:


video_ids_list=get_videos_ids(playlist_id_value)

pprint(video_ids_list)


# In[15]:


# TO GET each VIDEO DETAILS:  

def video_details(video_ids_list):
    video_info_list = []
    for i in video_ids_list:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=i
        )
        response = request.execute()
        if 'items' in response and response['items']:
            data = dict(Channel_Name=response['items'][0]['snippet']['channelTitle'],
                Channel_Id=response['items'][0]['snippet']['channelId'],
                video_id=response['items'][0]['id'],
                video_title=response['items'][0]['snippet']['title'],
                video_description=response['items'][0]['snippet']['description'],
                tags=response['items'][0]['snippet'].get('tags', []),
                pAt=response['items'][0]['snippet']['publishedAt'],
                comment_count=response['items'][0]['statistics'].get('commentCount', []),
                like_count=response['items'][0]['statistics']['likeCount'],
                view_count=response['items'][0]['statistics']['viewCount'],
                fav_count=response['items'][0]['statistics']['favoriteCount'],
                thumbnail=response['items'][0]['snippet']['thumbnails'],
                duration=response['items'][0]['contentDetails']['duration']
            )
            video_info_list.append(data)
        else:
            video_info_list.append(None)

    return video_info_list


# In[16]:


VD=video_details(video_ids_list)
pprint(VD)


# In[17]:


def comment_details(video_ids_list):
    all_com = []
    for j in video_ids_list:
        
        try: 
            request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=j)
            response_com = request.execute()

            for i in range (len(response_com['items'])):
                data_comments = dict(
                Comment_Id=response_com['items'][i]['snippet']['topLevelComment']['snippet']['authorChannelId']['value'],
                Comment_Author=response_com['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                Comment_Text=response_com['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                Comment_PublishedAt=response_com['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'],
                video_Id=response_com['items'][i]['snippet']['topLevelComment']['snippet']['videoId'])
                all_com.append(data_comments)

                          
        except :
            None

    return all_com          


# In[18]:


cm_d=comment_details(video_ids_list)
pprint(cm_d)


# In[19]:


def data_harvest (channel_id):
    _id= channel_id
    channel_data=channel_details(channel_id)
    playlist_details=get_videos_ids(playlist_id_value)
    video_detail=video_details(video_ids_list)
    comment_details_data = comment_details(video_ids_list) 
        
    data= {
    'Channel Data': channel_data,
    'Video Data': video_detail,
    'Comment_Data': comment_details_data 
    }

    return data


# In[20]:


data_harvest('UCABUpiUH2LbLmWu_i2CTFRA')


# In[21]:


from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
newdb = client['Youtube_data_harvest']
newcol = newdb['channel_data']


# In[22]:


def mongo_data_transfer(channel_id):
    channel_data =ch_d
    playlist_details = playlist_id_value
    video_detail = VD
    comment_details_data = cm_d

    channel= {
        'Channel Data': channel_data,
        'Playlist Data': playlist_details,
        'Video Data': video_detail,
        'Comment Data': comment_details_data
    }

    newcol.insert_one(channel) 


# In[23]:


mongo_data_transfer('UCABUpiUH2LbLmWu_i2CTFRA')


# In[2]:


import pymysql
import pandas as pd

connection_params = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Nahid123',
}
connection = pymysql.connect(**connection_params)
cursor = connection.cursor()
database_name = 'Youtube_data_harvest'
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
connection = pymysql.connect(host= 'localhost',user = 'root',password = 'Nahid123',database = 'Youtube_data_harvest')
cursor = connection.cursor()


# In[5]:


def channels_table():
    drop_query='''DROP TABLE IF EXISTS channels'''
    cursor.execute(drop_query)
    connection.commit()
    create_query = '''CREATE TABLE IF NOT EXISTS channels (Channel_Name VARCHAR(100),
                                                            Channel_Id VARCHAR(100) PRIMARY KEY,
                                                            Subscription_Count BIGINT,
                                                            view_count BIGINT,
                                                            Channel_Description TEXT,
                                                            Channel_pAt VARCHAR(50),
                                                            Playlist_Id VARCHAR(50))'''
                                                        
                                                        
    cursor.execute(create_query)
    connection.commit()
    
    ch_list = []
    db = client['Youtube_data_harvest']
    newcol = db['channel_data']

    for c in newcol.find({}, {'_id': 0, 'Channel Data': 1}):
        ch_list.append(c['Channel Data'])

    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''
        INSERT INTO channels(Channel_Name,
                                Channel_Id,
                                Subscription_Count,
                                view_count,
                                Channel_Description,
                                Channel_pAt,
                                Playlist_Id)
        VALUES(%s,%s,%s,%s,%s,%s,%s)
        '''

        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Subscription_Count'],
            row['view_count'],
            row['Channel_Description'],
            row['Channel_pAt'].replace("T","").replace("Z",""),
            row['Playlist_Id']
        )

        cursor.execute(insert_query, values)
        connection.commit()




# In[6]:


channels_table()


# In[8]:


def videos_table():
    try:
        drop_query='''DROP TABLE IF EXISTS videos'''
        cursor.execute(drop_query)

        cursor.execute('''CREATE TABLE IF NOT EXISTS videos(
                                                            Channel_Name VARCHAR(500),
                                                            Channel_Id VARCHAR(50),
                                                            video_id VARCHAR(50) PRIMARY KEY,
                                                            video_title VARCHAR(500),
                                                            video_description VARCHAR(5000),
                                                            tags VARCHAR(500),
                                                            pAt VARCHAR(100),
                                                            comment_count BIGINT,
                                                            like_count BIGINT,
                                                            view_count BIGINT,
                                                            fav_count INT,
                                                            thumbnail VARCHAR(100),
                                                            duration VARCHAR(100))''')

        vi_list = []
        db = client['Youtube_data_harvest']
        newcol = db['channel_data']
        for v in newcol.find({}, {'_id': 0, 'Video Data': 1}):
            for i in range(len(v['Video Data'])):
                vi_list.append(v['Video Data'][i])
        df2 = pd.DataFrame(vi_list)

        for index, row in df2.iterrows():
            insert_query= '''INSERT INTO videos(Channel_Name,Channel_Id,video_id,video_title,video_description,tags,
                            pAt,comment_count,like_count,view_count,fav_count,thumbnail,duration)
                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['video_id'],
                    row['video_title'],
                    row['video_description'],
                    ",".join(row['tags']),
                    row['pAt'].replace('T','').replace('Z',''),
                    row['comment_count'],
                    row['like_count'],
                    row['view_count'],
                    row['fav_count'],
                    row['thumbnail']['default']['url'],
                    row['duration'].replace('PT', '').replace('H', ':').replace('M', ':').split('S')[0])         
            cursor.execute(insert_query,values) 
        connection.commit() 
    except:
        connection.commit()
#         print("data transfer successfull")


# In[9]:


videos_table()


# df2

# In[3]:


def comments_table():
    drop_query='''DROP TABLE IF EXISTS comments'''
    cursor.execute(drop_query)
    connection.commit()
    create_query = '''CREATE TABLE IF NOT EXISTS comments(Comment_Id varchar(100) primary key,
                                                                  video_id VARCHAR(50),
                                                                  Comment_Text VARCHAR(1000),
                                                                  Comment_Author VARCHAR(500),
                                                                  Comment_PublishedAt VARCHAR(100)
                                                                  )'''

    cursor.execute(create_query)
    connection.commit()

    com_list = []
    db = client['Youtube_data_harvest']
    newcol = db['channel_data']
    for c in newcol.find({}, {'_id': 0, 'Comment Data': 1}):
        for i in range(len(c['Comment Data'])):
            com_list.append(c['Comment Data'][i])
    df3 = pd.DataFrame(com_list)

    insert_query = '''INSERT IGNORE INTO comments(
                          Comment_Id,
                          video_Id,
                          Comment_Text,
                          Comment_Author,
                          Comment_PublishedAt)
                      VALUES (%s, %s, %s, %s, %s)'''


    for index, row in df3.iterrows():
        values = (
            row['Comment_Id'],
            row['video_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            row['Comment_PublishedAt']
        )

        cursor.execute(insert_query, values)

    connection.commit()


# In[4]:


comments_table()


# In[24]:


def tables():
    channels_table()
    videos_table()
    comments_table()

    return 'Tables created successfully'


# In[ ]:





# In[ ]:




