## Overview

[Oracle XStream](https://docs.oracle.com/database/121/XSTRM/xstrm_intro.htm#XSTRM72647) is a mechanism for applications 
to recieve changes from an Oracle database in real time. This functionality is very similar to [Oracle GoldenGate](http://www.oracle.com/technetwork/middleware/goldengate/overview/index.html),
enough so that it requires a GoldenGate license. 

## Installation

The Oracle XStream API requires the Oracle OCI (Think) Client. Download the proper client from 
[Oracle Instant Client downloads](http://www.oracle.com/technetwork/topics/intel-macsoft-096467.html).

Download [instantclient-basic-macos.x64-12.1.0.2.0.zip](http://www.oracle.com/technetwork/topics/intel-macsoft-096467.html)

Download [instantclient-basic-macos.x64-11.2.0.4.0.zip](http://www.oracle.com/technetwork/topics/intel-macsoft-096467.html)


### Install artifacts in local repository

#### Oracle 11g

```
mvn install:install-file -DgroupId=com.oracle -DartifactId=ojdbc6 -Dversion=11.2.0.4 -Dpackaging=jar -Dfile=oracle/instantclient_11_2/ojdbc6.jar
mvn install:install-file -DgroupId=com.oracle -DartifactId=xstreams -Dversion=11.2.0.4 -Dpackaging=jar -Dfile=oracle/instantclient_11_2/xstreams.jar
```

#### Oracle 12c

```
mvn install:install-file  -DgroupId=com.oracle -DartifactId=ojdbc6 -Dversion=12.1.0.2 -Dpackaging=jar -Dfile=oracle/instantclient_12_1/ojdbc6.jar
mvn install:install-file  -DgroupId=com.oracle -DartifactId=xstreams -Dversion=12.1.0.2 -Dpackaging=jar -Dfile=oracle/instantclient_12_1/xstreams.jar
```

### Upload artifacts to Nexus

#### Oracle 11g

```
export NEXUS_URL='http://nexus-01:8081/repository/maven-releases/'
export NEXUS_REPO_ID='ldap-jeremy'
mvn deploy:deploy-file -DrepositoryId=$NEXUS_REPO_ID -Durl=$NEXUS_URL -DgeneratePom=true -Dpackaging=jar -DgroupId=com.oracle -DartifactId=ojdbc6 -Dversion=11.2.0.4 -Dfile=oracle/instantclient_11_2/ojdbc6.jar
mvn deploy:deploy-file -DrepositoryId=$NEXUS_REPO_ID -Durl=$NEXUS_URL -DgeneratePom=true -Dpackaging=jar -DgroupId=com.oracle -DartifactId=xstreams -Dversion=11.2.0.4 -Dfile=oracle/instantclient_11_2/xstreams.jar
```


#### Oracle 12c

```
export NEXUS_URL='https://nexus.custenborder.com/repository/maven-releases/'
export NEXUS_REPO_ID='ldap-jeremy'
mvn deploy:deploy-file -DrepositoryId=$NEXUS_REPO_ID -Durl=$NEXUS_URL -DgeneratePom=true -Dpackaging=jar -DgroupId=com.oracle -DartifactId=ojdbc6 -Dversion=12.1.0.2 -Dfile=oracle/instantclient_12_1/ojdbc6.jar
mvn deploy:deploy-file -DrepositoryId=$NEXUS_REPO_ID -Durl=$NEXUS_URL -DgeneratePom=true -Dpackaging=jar -DgroupId=com.oracle -DartifactId=xstreams -Dversion=12.1.0.2 -Dfile=oracle/instantclient_12_1/xstreams.jar
```