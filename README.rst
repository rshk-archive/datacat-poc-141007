Data Catalog - PoC 2014-10-07
#############################

Data catalog offering extra functionality on top of the indexed data.

Note that, differently from the 140825 PoC, this application *does
not* offer functionality for importing the data; that job is left to a
separate application (that will be based on the `OpenData harvester
<http://git.io/harvester>`_).


Project status
==============

**Travis CI build status**

+----------+--------------------------------------------------------------------------------------+
| Branch   | Status                                                                               |
+==========+======================================================================================+
| master   | .. image:: https://travis-ci.org/rshk/datacat-poc-141007.svg?branch=master           |
|          |     :target: https://travis-ci.org/rshk/datacat-poc-141007                           |
+----------+--------------------------------------------------------------------------------------+
| develop  | .. image:: https://travis-ci.org/rshk/datacat-poc-141007.svg?branch=develop          |
|          |     :target: https://travis-ci.org/rshk/datacat-poc-141007                           |
+----------+--------------------------------------------------------------------------------------+


Project documentation
=====================

Documentation is hosted on GitHub pages:
http://rshk.github.io/datacat-poc-141007/


Technology
==========

The application is written in **Python** (2.7), using **Flask** as web
framework.

The main database is **PostgreSQL** (9.3+), which is accessed via
**Psycopg2**.

The data sources are then accessed using various libraries, depending
on the data source itself.



..
   Technology
   ==========

   The application is written in **Python** (2.7), based on **Flask**.

   It uses **PostgreSQL** (9.3+) as main storage, via **Psycopg2**.

   It also uses **Celery** for running async tasks, which in turn requires
   a message broker, such as **RabbitMQ** or **Redis**.

   The geographical plugin (shipped with the core) requires a **PostGIS**
   enabled database. It also uses the **shp2pgsql** script to import
   ESRI Shapefiles and **Mapnik** to render tiles from geographical data.

   Supported versions
   ------------------

   - **Python:** 2.7
   - **PostgreSQL:** 9.3, 9.4

   Notes
   -----

   .. note:: In order to use Celery-based tasks (only required for
	     certain plugins), you'll also need to install and configure a
	     broker, such as RabbitMQ or Redis.

	     See `Celery - Brokers
	     <http://docs.celeryproject.org/en/latest/getting-started/brokers/index.html>`_
	     for more information.


   Example use cases
   =================

   **Tabular data:**

   - store original data as (one of) CSV/ODS/XLS/...
   - expose data as derivate formats:

     - normalize CSV dialect (comma as separator, double quotes, ..)
     - normalize charset (utf-8)
     - ODS/XLS/...
     - JSON (apply column types, ..)
     - Other custom formats (even on a per-dataset basis, eg. a CSV
       containing coordinates can be converted to a Shapefile)


   **Geographical data:**

   - store original data as (one of) Esri Shapefile, GML, KML, GeoJSON..
   - import the data in a PostGIS table, to allow further queries /
     transformations.
   - expose data as other formats: SHP, GML, KML, GeoJSON, ..
   - use mapnik to render tile layers, according to a configuration
   - clean up the data before republishing:

     - use utf-8 as encoding for the text
     - use a standard projection for the data (WGS48) (?)
     - make sure output shapefiles have projection specification (PRJ
       file, in WKT format) [hint: use `gdalsrsinfo
       <http://www.gdal.org/gdalsrsinfo.html>`_ to convert proj description]


   **Textual data:**

   For example, PDFs, ODT, MS DOC, ...

   - Extract plain text
   - Index the plain text, offer full-text search functionality
   - Convert to HTML to view in the browser


   **Custom, per-dataset, transforms:**

   For example, we can create new datasets by aggregating other resources, ...


   The RESTful API
   ===============

   The API v1 exposes two different kinds of endpoints:

   - ``/api/1/admin`` -> the administrative API
   - ``/api/1/data`` -> the public API


   The administrative API
   ----------------------

   The `administrative API
   <http://rshk.github.io/datacat-poc-141007/api/admin.html>`_ is used to
   manage resources and dataset configurations.

   Usually, it would be protected by some authentication / authorization
   layer in case the service is exposed to the public "as-is".


   The public API
   --------------

   The `public API
   <http://rshk.github.io/datacat-poc-141007/api/public.html>`_ is
   entirely managed by plugins, which expose their own endpoints to
   republish the datasets in some way.


   Background service
   ==================

   The background task execution is implemented using Celery_; periodic
   tasks are scheduled via `Celery Beat`_.

   .. _Celery: http://www.celeryproject.org/
   .. _Celery Beat: http://docs.celeryproject.org/en/latest/userguide/periodic-tasks.html


   Usage
   =====

   .. note:: a more appropariate configuration method will be added later on

   The ``DATACAT_SETTINGS`` variable can be used to point to the
   (filesystem) path to a Python module containing configuration
   overrides.

   Create a "launcher" script:

   .. code-block:: python

       from datacat.core import app

       # Configure
       # app.config['DATABASE'] = ...

       # To create database:
       # from datacat.db import create_db
       # create_db(app.config)

       # Run the webapp
       app.run()
