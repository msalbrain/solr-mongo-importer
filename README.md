# solr-mongo-importer



### **To Run**
`install go v1.8`
`clone repo`
`run go mod tidy`

`go build main.go then run executable produced
`
or

`go run main.go`

This is application works similarly to mongo-connector for solr
The first mongolizer() checkes for if solr is upto date with
the data present in mongo replica set i.e (len(my_core)) == (len(collection))
if false:
    check for difference and start updating after skipping len(my_core)
afterwards mongolizer() ends and NightWatch() starts
NightWatch looks out for any insert changes in oplog and makes respective update to solr.
