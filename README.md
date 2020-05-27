# geospatial
Some geospatial work

1. area_finder.py: using rtree for indexing geospatial objects and fast searching
2. 2gis_scraper.py: parallalize scraping using selenium with resource pooling for webdriver for efficiency
3. rev_coder.py: using Nominatim for reverse geocoding..

## instruction for running Nominatim in docker
 - step 1: install  nominatim-docker
 `git clone git@github.com:mediagis/nominatim-docker.git`
   and then
  `cd nominatim-docker/<version>`
 - step 2: `docker build -t nominatim .` 
          download and use link [gcc-states-latest.osm.pbf](https://download.geofabrik.de/asia/gcc-states.html)
 - step 3: `docker run -t -v "${PWD}:/data" nominatim  sh /app/init.sh /data/gcc-states-latest.osm.pbf postgresdata 4`

 - step 4: `docker run --restart=always -p 6432:5432 -p 7070:8080 -d --name nominatim -v "${PWD}:/data/postgresdata:/var/lib/postgresql/11/main" nominatim bash /app/start.sh`
         optional step to seprate db and service layering
- step 5: `docker run --restart=always -p 7070:8080 -d -v "${PWD}:/data/conf:/data" nominatim sh /app/startapache.sh`
- step 6: regular updates...
