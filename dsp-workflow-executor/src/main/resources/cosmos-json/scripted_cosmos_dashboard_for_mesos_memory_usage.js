var ARGS;

var rows = [
{
"collapse": false,
"height": "250px",
"panels": [
{
"aliasColors": {},
"bars": true,
"dashLength": 10,
"dashes": false,
"datasource": null,
"fill": 1,
"id": 9,
"legend": {
"alignAsTable": true,
"avg": true,
"current": true,
"max": true,
"min": true,
"show": true,
"total": false,
"values": true
},
"lines": true,
"linewidth": 1,
"links": [],
"nullPointMode": "null",
"percentage": false,
"pointradius": 5,
"points": false,
"renderer": "flot",
"seriesOverrides": [],
"spaceLength": 10,
"span": 12,
"stack": false,
"steppedLine": false,
"targets": [
{
"aggregator": "avg",
"alias": "Memory Max",
"currentTagKey": "",
"currentTagValue": "",
"downsampleAggregator": "avg",
"downsampleFillPolicy": "none",
"metric": ARGS.appId + ".Memory.max",
"refId": "A",
"tags": {
"partitionValues": ARGS.partitionValues
"pipelineStepId":  ARGS.pipelineStepId
"requestId": ARGS.requestId
}
},
{
"aggregator": "avg",
"alias": "Memory Usage",
"currentTagKey": "",
"currentTagValue": "",
"downsampleAggregator": "avg",
"downsampleFillPolicy": "none",
"metric": ARGS.appId + ".Memory.usage",
"refId": "B",
"tags": {
"partitionValues": ARGS.partitionValues
"pipelineStepId":  ARGS.pipelineStepId
"requestId": ARGS.requestId
}
}
],
"thresholds": [],
"timeFrom": null,
"timeShift": null,
"title": "Max vs Used Memory",
"tooltip": {
"shared": true,
"sort": 0,
"value_type": "individual"
},
"type": "graph",
"xaxis": {
"buckets": null,
"mode": "time",
"name": null,
"show": true,
"values": []
},
"yaxes": [
{
"format": "decbytes",
"label": null,
"logBase": 1,
"max": null,
"min": null,
"show": true
},
{
"format": "short",
"label": null,
"logBase": 1,
"max": null,
"min": null,
"show": true
}
]
}
],
"repeat": null,
"repeatIteration": null,
"repeatRowId": null,
"showTitle": true,
"title": "Max Memory Vs Used Memory",
"titleSize": "h6"
}
]


// return dashboard
return {
"title": "DSP Job Memory Metrics Testing 1",
"style": "dark",
"time": {
"from": ARGS.from,
"to": ARGS.to
},
"refresh": "60s",
"rows": rows
};