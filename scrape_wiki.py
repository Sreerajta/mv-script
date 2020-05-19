#################################################################################################################################################################
#wikipedia has pages with list of movies by year , this scripts takes in an url and add the movie titles from the list to a db
#################################################################################################################################################################


import asyncio
import wikipedia
import requests
from bs4 import BeautifulSoup
import time
import re

import aiohttp
from aiohttp import ClientSession



from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship


SQLALCHEMY_DATABASE_URL = "sqlite:///./app.db" #change to read from config file if using other db

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

db=SessionLocal()

#movies_list table model
class MoviesList(Base):
    __tablename__ = "movies_list"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, unique=False, index=True)
    year = Column(Integer)
    has_data = Column(Boolean, default=False)


Base.metadata.create_all(bind=engine)



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
    html_text = await fetch_html(year_url,session)                                  #TODO:handle errors here

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
    # print(movie_titles)
    print('inserting data for year',year)
    values = []
    for title in movie_titles:
        values.append(MoviesList(
            title=title,
            year=year
        ))
        db.bulk_save_objects(values)
        db.commit()
    


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

