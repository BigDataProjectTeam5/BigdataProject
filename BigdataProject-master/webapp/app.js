'use strict';
const http = require('http');
const express = require('express');
const app = express();
const mustache = require('mustache');
const fs = require('fs');
const cassandra = require('cassandra-driver');
const client = new cassandra.Client({
  contactPoints: [process.argv[3]],
  localDataCenter: 'datacenter1',
  keyspace: 'your_keyspace_name'
});
const port = Number(process.argv[2]);

app.use(express.static('public'));
app.get('/traffic.html', function (req, res) {
  client.execute('SELECT * FROM yson_streets', [], { prepare: true })
    .then(result => {
      var template = fs.readFileSync("submit.mustache").toString();
      var html = mustache.render(template, {
        streets: result.rows
      });
      res.send(html)
    })
    .catch(error => {
      console.error(error);
      res.status(500).send("Internal Server Error");
    });
});

function removePrefix(text, prefix) {
  return text.substr(prefix.length)
}

function byteToInt(x) {
  let val = 0;
  for (let i = 0; i < x.length; ++i) {
    val += x[i];
    if (i < x.length - 1) val = val << 8;
  }
  return val;
}

function counterToNumber(c) {
  return Number(Buffer.from(c).readInt32BE());
}

app.get('/street-results.html', function (req, res) {
  const street = req.query['street'];
  console.log(street); 


function processSegmentIdRecord(segmentIdRecord) {
	var result = { segment_id : segmentIdRecord['segment_id']};
	["from_street", "to_street", "traffic_direction",
		"speed_month", "speed_week", "speed_day", "speed_hour", "speed_now"].forEach(val => {
		if (val == "speed_now") {
			if (counterToNumber(segmentIdRecord[val]) != 0) {
				result[val] = counterToNumber(segmentIdRecord[val]);
			} else {
				result[val] = "-"
			}
		} else {
			result[val] = segmentIdRecord[val];
		}
	})
	return result;
}
function SpeedInfo(cells) {
	var result = [];
	var segmentIdRecord;
	cells.forEach(function(cell) {
		var segment_id = Number(removePrefix(cell['key'], street))
		if(segmentIdRecord === undefined)  {
			segmentIdRecord = { segment_id: segment_id }
		} else if (segmentIdRecord['segment_id'] != segment_id ) {
			result.push(processSegmentIdRecord(segmentIdRecord))
			segmentIdRecord = { segment_id: segment_id }
		}
		segmentIdRecord[removePrefix(cell['column'],'stats:')] = cell['$']
	})
	result.push(processSegmentIdRecord(segmentIdRecord))
	return result;
}
function processRedlightSpeedRecord(streetRecord) {
	var result = { street : streetRecord['street_name']};
	["redlight_year", "redlight_months", "speed_year", "speed_months"].forEach(val => {
		if (streetRecord[val] === undefined) {
			result[val] = "-"
		} else {
			result[val] = streetRecord[val];
		}
	})
	return result;
}
function RedlightSpeedInfo(cells) {
	var result = [];
	var streetRecord;
	// console.log(streetRecord)
	cells.forEach(function(cell) {
		var street_name = cell['key']

		if(streetRecord === undefined)  {
			streetRecord = { street_name: street_name }
			// console.log(streetRecord)
		} else if (streetRecord['street_name'] != street_name ) {
			result.push(processRedlightSpeedRecord(streetRecord))
			streetRecord = {street_name: street_name}
		}
		streetRecord[removePrefix(cell['column'],'stats:')] = cell['$']
	})
	result.push(processRedlightSpeedRecord(streetRecord))
	return result;
}
function processCrashRecord(crashIdRecord) {
	var result = { crash_record_id : crashIdRecord['crash_record_id']};
	["crash_date", "street", "address", "first_crash_type", "crash_type", "prim_cause", "damage"].forEach(val => {
		result[val] = crashIdRecord[val];
	})
	return result;
}
function CrashInfo(cells) {
	var result = [];
	var crashIdRecord
	cells.forEach(function(cell) {
		var crash_record_id = removePrefix(cell['key'], street)
		// console.log(crash_record_id)
		if(crashIdRecord === undefined)  {
			crashIdRecord = { crash_record_id: crash_record_id }
		} else if (crashIdRecord['crash_record_id'] != crash_record_id) {
			result.push(processCrashRecord(crashIdRecord))
			crashIdRecord = { crash_record_id: crash_record_id }
		}
		crashIdRecord[removePrefix(cell['column'],'stats:')] = cell['$']
	})
	result.push(processCrashRecord(crashIdRecord))
	return result;
}

  // Use client to execute your queries here
client.execute('SELECT * FROM yson_street_by_seg WHERE street = ?', [street], { prepare: true })
    .then(result => {
      // Process the result and render HTML
      res.send(html);
    })
    .catch(error => {
      console.error(error);
      res.status(500).send("Internal Server Error");
    });
});

app.listen(port);
