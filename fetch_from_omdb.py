################################################################################################
#CONSUMING FROM QUEUE, RUN MULTIPLE PROCESSES TO CONSUME FROM QUEUE"
################################################################################################ 
################################################################################################
#Reads movie titles from beanstalkd in memory queue and adds data to cassandra table"
################################################################################################ 
import asyncio
import aiohttp
from aiohttp import ClientSession
from cassandra.cluster import Cluster
import json
import greenstalk
import uuid 

queue = greenstalk.Client(host='127.0.0.1', port=11305)

###Edit according to cassandra config
cluster = Cluster(['127.0.0.1'])
cassandra_session = cluster.connect('default_keyspace')

API_KEY =''

_session = None


async def fetch_data(url: str, session:ClientSession,**kwargs):
    resp = await session.request(method="GET", url=url, **kwargs)
    resp.raise_for_status()
    response_data = await resp.text()
    if response_data:
        return json.loads(response_data)
    else:
        return None



async def fetch_add_movie_to_db(title:str,session:ClientSession,cr_num:int,**kwargs):
    url_1='http://www.omdbapi.com/?t='  
    url_2 ='&APIKEY=' + API_KEY
    
    url = url_1 + title + url_2

    print('cr num:'+ str(cr_num) + ' fetching data for ',title)

    movie_data = await fetch_data(url = url,session=session)
    
    query =  """
    INSERT INTO movies_list (id,genres, plot, poster,rating,title)
    VALUES (%s, %s, %s, %s, %s,%s)
    """
    #TODO:needs a bit of checking here,to make sure api returned data and returned data has the intended fields. P
    cassandra_session.execute(query,[uuid.uuid1(),movie_data['Genre'],movie_data['Plot'],movie_data['Poster'],float(movie_data['imdbRating']),movie_data['Title']])


async def process_it(cr_num:int,session:ClientSession):
    print('running cr:',cr_num)    
    while(True):
        job = queue.reserve()
        queue.delete(job)
        job = json.loads(job.body)
        # print('JOB:',job['title'])
        await fetch_add_movie_to_db(title=job['title'],session=session,cr_num=cr_num)

async def start_it():
    async with ClientSession() as session:
        tasks = []
        for i in range(3):
            tasks.append(process_it(cr_num=i,session=session))
        await asyncio.gather(*tasks)

if __name__ == "__main__":    
    asyncio.run(start_it())