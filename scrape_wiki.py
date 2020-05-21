#################################################################################################################################################################
#wikipedia has pages with list of movies by year , this scripts takes in an url and add the movie titles from the list to an in memory queue
#################################################################################################################################################################

#https://beanstalkd.github.io  - Beanstalk is a simple, fast work queue. 

import asyncio
import wikipedia
import requests
from bs4 import BeautifulSoup
import time
import re
import json

import aiohttp
from aiohttp import ClientSession

import greenstalk

queue = greenstalk.Client(host='127.0.0.1', port=11305) #edit here for beasntalk params

#film list is wikipedia is usually of the below format.. followed by an year, change the base url and modify the YEARS set
BASE_URL = 'https://en.wikipedia.org/wiki/List_of_American_films_of_'
YEARS = ('1950','1951')


def remove_parenthesis(title:str):
    new_title = re.sub("[\(\[].*?[\)\]]", "", title)
    return new_title


async def fetch_html(url: str, session: ClientSession, **kwargs):    
    resp = await session.request(method="GET", url=url, **kwargs)
    resp.raise_for_status()
    html = await resp.text()
    return html

async def parse_movie_titles(year:str,session:ClientSession,**kwargs):
    print('parsing titles for year',year)
    film_titles = []
    year_url = BASE_URL + year
    html_text = await fetch_html(year_url,session)                           

    b = BeautifulSoup(html_text, 'lxml')

    #get the table in the page, take the table rows, get the first column  which has the movie name which is an <a> tag
    for i in b.find_all(name='table', class_='wikitable'):
        for j in i.find_all(name='tr'):
            for k in j.find_all(name='i'):
                for link in k.find_all('a', href=True):                
                    film_titles.append(link['title'])    #strip the things written in bracket?????  we have the year anyways and omdb can take in year as well
    return film_titles


async def process_for_one_year(year:str,**kwargs):
    movie_titles = await parse_movie_titles(year,**kwargs)
    print('inserting data for year',year)
    values = []
    for title in movie_titles:
        print('adding to queue',title)
        data = json.dumps({"title":remove_parenthesis(title),"year":year})
        queue.put(data)

      
    


async def bulk_write(years:set, **kwargs):
    async with ClientSession() as session:
        tasks = []
        for year in years:
            tasks.append(
                process_for_one_year(year=year, session=session, **kwargs)
            )
        await asyncio.gather(*tasks)



if __name__ == "__main__":
    asyncio.run(bulk_write(years=YEARS))

