
# Lothbrok: Optimizing SPARQL Queries over Decentralized Knowledge Graphs
[![Docker Stars](https://img.shields.io/docker/stars/caebel/lothbrok.svg)](https://hub.docker.com/r/caebel/lothbrok/) [![Docker Stars](https://img.shields.io/docker/pulls/caebel/lothbrok.svg)](https://hub.docker.com/r/caebel/lothbrok/)

Lothbrok is a novel approach to optimizing SPARQL queries over decentralized knowledge graphs that fragments data using characteristic sets and considers the locality of data when processing queries. Lothbrok builds on top of [ColChain](https://github.com/ColChain/ColChain-Java) and [PIQNIC](https://github.com/Chraebe/PIQNIC). For more information, including the full experimental setup and results, please visit our [website](https://relweb.cs.aau.dk/lothbrok).
* [Abstract](#abstract)
* [Requirements](#requirements)
* [Installation](#installation)
* [Status](#status)
* [Running Experiments](#running-experiments)
# Abstract
While the Web of Data in principle offers access to a wide range of interlinked data, the architecture of the Semantic Web today relies mostly on the data providers to maintain access to their data through SPARQL endpoints. Several studies, however, have shown that such endpoints often experience downtime, meaning that the data they maintain becomes inaccessible. While decentralized systems based on Peer-to-Peer (P2P) technology have previously shown to increase the availability of knowledge graphs, even when a large proportion of the nodes fail, processing queries in such a setup can be an expensive task since data necessary to answer a single query might be distributed over multiple nodes. In this paper, we therefore propose an approach to optimizing SPARQL queries over decentralized knowledge graphs, called Lothbrok. While there are potentially many aspects to consider when optimizing such queries, we focus on three aspects: cardinality estimation, locality awareness, and data fragmentation. We empirically show that Lothbrok is able to achieve significantly faster query processing performance compared to the state of the art when processing challenging queries as well as when the network is under high load.
# Requirements
* ***Docker*** 20 or higher

*Or the following:*

* Java 8 or newer
* Maven
* Application server such as [Jetty](https://www.eclipse.org/jetty/) or [Tomcat](http://tomcat.apache.org/)
# Installation
## Install with Docker
To install from Docker, simply use the following command:
```
docker run -d -p <port>:8080 caebel/lothbrok
```
where `<port>` is the port you want your Lothbrok instance to be mapped to.

## Install without Docker
To install and run the implementation of Lothbrok, go to the directory for [ColChain](https://github.com/dkw-aau/Lothbrok-Java/tree/main/Lothbrok-ColChain)  or [PIQNIC](https://github.com/dkw-aau/Lothbrok-Java/tree/main/Lothbrok-Piqnic) and follow the guides there.
# Status
Lothbrok is currently a prototype implementation on top of two state-of-the-art systems. We are currently working on creating a standalone package that lets developers easily extend their own system with Lothbrok. Stay tuned!
# Running Experiments
To reproduce our experiments, please download the [resources](https://doi.org/10.5281/zenodo.6538999) that includes all the data, indexes, setup files, scripts etc., necessary to run our experiments and follow the guide for the setup in that repository.

