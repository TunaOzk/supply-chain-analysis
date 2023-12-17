
# Supply Chain Analysis

Through the analysis, the goal is to identify issues that arise in the order process, provide insights into the reasons behind these issues, improve the quality of service provided to customers, and at the same time, provide feedback that will allow organizations seeking to be part of the supply chain to position themselves according to the needs within the chain.


## Run Locally

### Requirements
- Apache Superset https://superset.apache.org/ 
- Apache Drill https://drill.apache.org/docs/ (v1.21.0)
- MongoDB

Clone the project

```bash
  git clone https://github.com/TunaOzk/supply-chain-analysis.git
```

Navigate to the Drill installation directory. Start Drill on Terminal. 

```bash
  bin/drill-embedded
```

Start Superset on another Terminal. Note that Drill and Superset should run on the same virtual enviroment. (Please check the Docs first.)

```bash
  superset run -p 8088 --with-threads --reload --debugger
```
Now you are ready to analyze your supply chain!

Some Notes:
- Adding new analyses --> Analyses.scala
- Edit the DB Configurations --> DatabaseConnection.scala



## Authors
- [@OguzKaanOselmis](https://github.com/OguzKaanOselmis)
- [@TunaOzk](https://github.com/TunaOzk)


