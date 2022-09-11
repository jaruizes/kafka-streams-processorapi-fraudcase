#!/bin/bash

TODAY=$(date +"%Y-%m-%d")

#ONLINE
curl --location --request POST 'http://localhost:8090/movement' --header 'Content-Type: application/json' --data-raw '{"id":"case_2-m1","card":"c2","amount":10.0,"origin": 3,"site": "site1", "device": "", "createdAt": "'$TODAY' 11:47:05 CEST"}'
curl --location --request POST 'http://localhost:8090/movement' --header 'Content-Type: application/json' --data-raw '{"id":"case_2-m2","card":"c2","amount":90.0,"origin": 3,"site": "site2", "device": "", "createdAt": "'$TODAY' 11:47:15 CEST"}'
curl --location --request POST 'http://localhost:8090/movement' --header 'Content-Type: application/json' --data-raw '{"id":"case_2-m3","card":"c2","amount":200.0,"origin": 3,"site": "site3", "device": "", "createdAt": "'$TODAY' 11:47:35 CEST"}'
curl --location --request POST 'http://localhost:8090/movement' --header 'Content-Type: application/json' --data-raw '{"id":"case_2-m4","card":"c2","amount":100.0,"origin": 3,"site": "site4", "device": "", "createdAt": "'$TODAY' 11:47:55 CEST"}'

#curl --location --request POST 'http://localhost:8090/movement' --header 'Content-Type: application/json' --data-raw '{"id":"case_9-m1","card":"c9","amount":100.0,"origin": 3,"site": "site1", "device": "", "createdAt": "'$TODAY' 11:47:05 CEST"}'
#curl --location --request POST 'http://localhost:8090/movement' --header 'Content-Type: application/json' --data-raw '{"id":"case_9-m2","card":"c9","amount":190.0,"origin": 3,"site": "site2", "device": "", "createdAt": "'$TODAY' 11:47:15 CEST"}'
#curl --location --request POST 'http://localhost:8090/movement' --header 'Content-Type: application/json' --data-raw '{"id":"case_9-m3","card":"c9","amount":200.0,"origin": 3,"site": "site3", "device": "", "createdAt": "'$TODAY' 11:47:35 CEST"}'
#curl --location --request POST 'http://localhost:8090/movement' --header 'Content-Type: application/json' --data-raw '{"id":"case_9-m4","card":"c9","amount":100.0,"origin": 3,"site": "site4", "device": "", "createdAt": "'$TODAY' 11:47:55 CEST"}'

#NO FRAUD
curl --location --request POST 'http://localhost:8090/movement' --header 'Content-Type: application/json' --data-raw '{"id":"case_3-m1","card":"c2","amount":10.0,"origin": 3,"site": "site1", "device": "", "createdAt": "'$TODAY' 12:10:05 CEST"}'
curl --location --request POST 'http://localhost:8090/movement' --header 'Content-Type: application/json' --data-raw '{"id":"case_3-m2","card":"c2","amount":90.0,"origin": 3,"site": "site2", "device": "", "createdAt": "'$TODAY' 12:10:15 CEST"}'
curl --location --request POST 'http://localhost:8090/movement' --header 'Content-Type: application/json' --data-raw '{"id":"case_3-m3","card":"c2","amount":20.0,"origin": 3,"site": "site3", "device": "", "createdAt": "'$TODAY' 12:10:35 CEST"}'
curl --location --request POST 'http://localhost:8090/movement' --header 'Content-Type: application/json' --data-raw '{"id":"case_3-m4","card":"c2","amount":49.0,"origin": 3,"site": "site4", "device": "", "createdAt": "'$TODAY' 12:10:55 CEST"}'

echo ""
echo "-------------------------------"
echo "Topic movimientos: http://localhost:9081/ui/clusters/local/topics/movements"
echo "Topic casos de fraude: http://localhost:9081/ui/clusters/local/topics/fraud-cases"
echo "-------------------------------"