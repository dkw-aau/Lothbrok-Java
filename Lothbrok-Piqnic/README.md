# Piqnic: a P2p system for Query processiNg over semantIC data
A github repository for PIQNIC Version 0.2-SNAPSHOT
* [Abstract](#abstract)
* [Requirements](#requirements)
* [Installation](#installation)
	* [Build](#build)
	* [Deploy](#deploy)
	* [Setup](#setup)
* [Status](#status)
* [Running Experiments](#running-experiments)
# Abstract
### Research paper
Although the Semantic Web in principle provides access to a vast Web of interlinked data, the full potential currently remains mostly unexploited. One of the main reasons is the fact that the architecture of the current Web of Data relies on a set of servers providing access to the data. These servers represent bottlenecks and single points of failure that result in instability and unavailability of data at certain points in time. In this paper, we therefore propose a decentralized architecture (Piqnic) for sharing and querying semantic data. By combining both client and server functionality at each participating node and introducing replication, Piqnic avoids bottlenecks and keeps datasets available and queryable although the original source might (temporarily) not be available. Our experimental results using a standard benchmark of real datasets show that Piqnic can serve as an architecture for sharing and querying semantic data, even in the presence of node failures.
# Requirements
* Java 8 or newer
* Maven
* Application server such as [Jetty](https://www.eclipse.org/jetty/) or [Tomcat](http://tomcat.apache.org/)
# Installation
### Build
To install and run a Piqnic node, build the project using Maven:
```
mvn install
```
Or to create a runnable Jar with dependencies:
```
mvn compile assembly:single
```
### Deploy
To run a Piqnic node, you must create a Config file. Here is an example config.json file:
```json
{
  "title": "My Piqnic Client",
  "datasourcetypes": {
    "HdtDatasource": "org.linkeddatafragments.datasource.hdt.HdtDataSourceType"
  },

  "datastore":"datastore/",
  "address":"http://piqnic.org:8080",
  "prefixes": {
    "rdf":         "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs":        "http://www.w3.org/2000/01/rdf-schema#",
    "xsd":         "http://www.w3.org/2001/XMLSchema#",
    "dc":          "http://purl.org/dc/terms/",
    "foaf":        "http://xmlns.com/foaf/0.1/",
    "dbpedia":     "http://dbpedia.org/resource/",
    "dbpedia-owl": "http://dbpedia.org/ontology/",
    "dbpprop":     "http://dbpedia.org/property/",
    "hydra":       "http://www.w3.org/ns/hydra/core#",
    "void":        "http://rdfs.org/ns/void#"
  }
}
```
Once Piqnic has been built, it can be deployed as standalone or in an application server.
To run a Piqnic node standalone, use the following command:
```
java -jar piqnic.jar config.json
```
To deploy in an application server, use the WAR file. Create an `config.json` configuration file with the data sources (analogous to the example file) and add the following init parameter to `web.xml`. If no parameter is set, it looks for a default `config-example.json` in the folder of the deployed WAR file.
```xml
<init-param>
  <param-name>configFile</param-name>
  <param-value>path/to/config/file</param-value>
</init-param>
```
### Setup
Once the node is started, go to the web interface by opening a Web browser and navigating to the URL, e.g., `http://piqnic.org:8080/`. Enter the path to the config file in the Web interface (1st form from the top), e.g., `/path/to/config.json` and hit `Initiate`.
For experimental setup of a node, refer to [Running Experiments](#running-experiments).
# Status
Piqnic is currently implemented as a prototype used for experiments in the Piqnic paper. We are working on creating making the following features available in Piqnic:
* Different RDF compression techniques
* User-chosen indexing schema
* More fragmentation functions
* Easier to use user interface

Stay tuned for more such features!
# Running Experiments
### Creating a Setup
To create the setup directories, navigate in the Web browser to the URL, e.g., `http://piqnic.org:8080/`. In the Web interface, enter the following (3rd form from the top):
* config file, e.g., `/path/to/config.json`
* data directory, e.g., `/path/to/data/`
* number of nodes in total, e.g., `128`
* replications per fragment (number of participants in each community), e.g., `10`

This will create the `setup` directory where all the needed files are located.
### Running a Setup
To setup a node for experiments, go to the Web interface by navigating to the correct URL, e.g., `http://piqnic.org:8080/`. Enter the following (4th form from the top):
* config file, e.g., `/path/to/config.json`
* data directory, e.g., `/path/to/data/`
* setup directory, e.g., `/path/to/setup/` (see above)
* node ID, e.g., `0`
* number of nodes in total, e.g., `128`
* chain lengths, e.g., `100`

### Running Experiments
Now that the node is setup, use the Web interface to start the experiments (under the item "Experiments") by filling out the form with the relevant fields for the relevant experiments.

