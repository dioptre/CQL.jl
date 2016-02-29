CQL.jl
======

Cassandra CQL for julia. Please note if you are looking for the old driver please try: https://github.com/wiffel/cql.js


API
===

    function connect(srv::String = "localhost", prt::Int = 9042)
    function disconnect(con::CQLConnection)

To open and close a connection to the Cassandra server.

    function query(con::CQLConnection, msg::String)

'Normal' Synchronous Query
Will wait for until all scheduled commands have been executed
Will then send the query and wait for the result.
The processed result is returned as an array or
rows, which are themselves array with the values
for the requested columns.

    function command(con::CQLConnection, msg::String)

The same as 'query', but we don't get the result back.
Is a bit faster and uses less memory, because the
reply from the server is neglected.

    function asyncQuery(con::CQLConnection, msg::String)

An Asynchronous Query
Will send the query to the server and returns 
with a 'future'. After the server has processed the
query and did send back the result, the 'future' will
contain the result. This result can be fetched with
'getResult', which gives back the result in 
the same format as 'query'.

    function asyncCommand(con::CQLConnection, msg::String)

The same as 'command', but asynchronous.
It sends of the command to the server instantly and
neglects the response.
This is the fastest way to execute cql commands, but
no garantees can be given on e.g. the order in which
commands are being executed by the server.

    function getResult(reply)

To fetch the result from a call by asyncQuery.
Will block if the result is not there yet.

    function sync(con)

Not very usefull, but waits until all messages
that were send to the server where processed and
communicated back.
Can be handy to synchronize or do correct timig tests.

##Help you get started

```

sudo add-apt-repository ppa:staticfloat/julianightlies
sudo add-apt-repository ppa:staticfloat/julia-deps
sudo apt-get install julia

sudo apt-get install curl software-properties-common
 
# install java
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer
sudo apt-get install libjna-java

# install
echo "deb http://debian.datastax.com/community stable main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
curl -L http://debian.datastax.com/debian/repo_key | sudo apt-key add -
sudo apt-get update
sudo apt-get install dsc30

# setup
sudo service cassandra stop
sudo rm -rf /var/lib/cassandra/*
in "/etc/cassandra/cassandra.yaml":
 rpc_address: 0.0.0.0
 rpc_enabled: true
sudo service cassandra start

# check
nodetool status

# uninstall
sudo service cassandra stop
sudo apt-get autoremove dsc20
sudo rm -rf /var/lib/cassandra

#############################################

# setup demo db

cqlsh
> CREATE KEYSPACE demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
> USE demo;
> CREATE TABLE users (id int PRIMARY KEY, firstname text, lastname text);
> INSERT INTO users (id,  firstname, lastname) VALUES (111, 'john', 'smith');
> INSERT INTO users (id,  firstname, lastname) VALUES (222, 'john', 'smith');
> INSERT INTO users (id,  firstname, lastname) VALUES (333, 'john', 'smith');
> SELECT * FROM users;
> CREATE INDEX ON users (firstname);
> CREATE INDEX ON users (lastname);

#############################################

# python install
sudo apt-get install python-dev
sudo apt-get install python-pip
sudo pip install cassandra-driver
sudo pip install blist

#############################################

python

from cassandra.cluster import Cluster
cluster = Cluster()
cluster = Cluster(['tilient.net'])
session = cluster.connect('demo')

rows = session.execute('SELECT * FROM users')
for p in rows:  print p

stmt = session.prepare("SELECT * FROM users WHERE id = ?")  
for id in [111, 222]:
    user = session.execute(stmt, [id])
    print(user)

```

#References
Created by
@wiffel @ https://github.com/wiffel/cql.js
CQL Native Protocol 3 Support & Julia v0.4 added by
@dioptre @ https://github.com/dioptre

