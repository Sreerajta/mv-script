- install  and start beanstalkd 
- edit scrape_wiki.py to add beasntalkd config
- edit  BASE_URL and YEARS in scrape_wiki.py according to the wikipedia pages containing the movies list
- run scrape_wiki.py to fetch and push movie titles into queue

- edit fetch_from_omdb.py to add cassandra config and omdb api key
- run fetch_from_omdb.py to consume from the beanstalk queue ,fetch data for the movies and add data to cassandra.


# OBVIOUS TODO: fetch_from_omdb breaks when data from omdb doesn't have any of the fields... lines to check it to be added