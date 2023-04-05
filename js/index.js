// const j = require("../data/timeseries_data_rawjson/2019_06_18_con_0a359cb0-8495-11e9-b08b-5bbf0adeb393.json")
// console.log(j.data.data)

const fs = require("fs");
const path = require("path");

const files = fs.readdirSync("../data/timeseries_data_rawjson");

const readings = [];
for (const file of files) {
  const data = fs.readFileSync(
    path.join("../data/timeseries_data_rawjson", file)
  );
  const json = JSON.parse(data);
  // filter out only the keys we want
  const filtered = json.data.data.map(
    ({ container_id, fill_level_percentage, timestamp }) => ({
      container_id,
      fill_level_percentage,
      timestamp,
    })
  );
  // console.log(json.data.data);
  readings.push(...filtered);
}
// console.log(readings.slice(0, 5));
// const readings_clean = readings.map;
console.log(JSON.stringify(readings));
