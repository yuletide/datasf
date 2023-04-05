const d = require("./readings.json");
function processTimeSeriesData(data) {
  const result = [];
  const alertThreshold = 80;
  const serviceThreshold = 20;
  const overflowThreshold = 100;
  const consecutiveReadings = 4;
  let alertTriggered = false;
  let serviceTriggered = false;
  let overflowTriggered = false;
  let lastAlertTime = null;
  let lastServiceTime = null;
  let lastOverflowTime = null;
  let lastFillLevel = null;
  let window;

  for (let i = 0; i < data.length; i++) {
    const d = data[i];
    const fillLevel = d.fill_level_percentage;

    if (fillLevel >= alertThreshold) {
      if (!alertTriggered) {
        lastAlertTime = d.timestamp;
        alertTriggered = true;
      } else if (i >= consecutiveReadings - 1) {
        window = data.slice(i - consecutiveReadings + 1, i + 1);
        const allAboveThreshold = window.every(
          (w) => w.fill_level_percentage >= alertThreshold
        );
        // console.log(allAboveThreshold);
        if (allAboveThreshold) {
          lastAlertTime = d.timestamp;
        } else {
          alertTriggered = false;
        }
      }
    } else {
      alertTriggered = false;
    }

    if (fillLevel <= serviceThreshold) {
      if (!serviceTriggered) {
        lastServiceTime = d.timestamp;
        serviceTriggered = true;
      } else if (i >= consecutiveReadings - 1) {
        window = data.slice(i - consecutiveReadings + 1, i + 1);
        const allBelowThreshold = window.every(
          (w) => w.fill_level_percentage <= serviceThreshold
        );
        if (allBelowThreshold) {
          lastServiceTime = d.timestamp;
        } else {
          serviceTriggered = false;
        }
      }
    } else {
      serviceTriggered = false;
    }

    if (alertTriggered && lastServiceTime) {
      const timeDiff = new Date(lastServiceTime) - new Date(lastAlertTime);
      const timeDiffInHours = timeDiff / (1000 * 60 * 60);
      result.push({
        container_id: d.container_id,
        alert_triggered_time: lastAlertTime,
        serviced_time: lastServiceTime,
        pick_up_time_hrs: timeDiffInHours.toFixed(2),
        overflow: overflowTriggered ? 1 : 0,
        readings: window,
      });
      alertTriggered = false;
      serviceTriggered = false;
      overflowTriggered = false;
      lastAlertTime = null;
      lastServiceTime = null;
      lastFillLevel = null;
    }

    if (fillLevel >= overflowThreshold) {
      if (!overflowTriggered) {
        lastOverflowTime = d.timestamp;
        overflowTriggered = true;
      } else if (i >= consecutiveReadings - 1) {
        const window = data.slice(i - consecutiveReadings + 1, i + 1);
        const allAboveThreshold = window.every(
          (w) => w.fill_level_percentage >= overflowThreshold
        );
        if (allAboveThreshold) {
          lastOverflowTime = d.timestamp;
        } else {
          overflowTriggered = false;
        }
      }
    } else {
      overflowTriggered = false;
    }
  }

  return result;
}
const processed = processTimeSeriesData(d);

console.log(JSON.stringify(processed));
