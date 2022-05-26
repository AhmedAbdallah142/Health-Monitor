console.log("Hello Web!");

const startDate = document.getElementById("start-date");
const startDateMinute = document.getElementById("Minute-1");
const endDate = document.getElementById("end-date");
const endDateMinute = document.getElementById("Minute-2");

const datatable = document.getElementById("data");

const sampleRes = JSON.parse(
  '[{"Name":"Health","Count":"2017","meanCPU":"0.4997","peakCPU":"01-04-2022","meanDisk":"0.5102","peakDisk":"01-04-2022","meanRAM":"0.3720","peakRAM":"01-04-2022"},{"Name":"Health1","Count":"6","meanCPU":"0.4916","peakCPU":"19-01-1970","meanDisk":"0.4433","peakDisk":"19-01-1970","meanRAM":"0.4583","peakRAM":"01-04-2022"},{"Name":"Health2","Count":"25","meanCPU":"0.4740","peakCPU":"01-04-2022","meanDisk":"0.5044","peakDisk":"01-04-2022","meanRAM":"0.42","peakRAM":"01-04-2022"}]'
);

function data_row(serviceData) {
  return `
  <tr>
    <td>${serviceData.Name}</td>
    <td>${serviceData.Count}</td>
    <td>${serviceData.meanCPU}</td>
    <td>${serviceData.peakCPU}</td>
    <td>${serviceData.meanRAM}</td>
    <td>${serviceData.peakRAM}</td>
    <td>${serviceData.meanDisk}</td>
    <td>${serviceData.peakDisk}</td>
  </tr>
  `;
}
function toTimestamp(strDate){
   var datum = Date.parse(strDate);
   return datum/1000;
}
let lock = false;
function getAnalysis() {
  const from = startDate.value.split("-").join("-").concat(" ", startDateMinute.value.concat(":00")),
    to = endDate.value.split("-").join("-").concat(" ", endDateMinute.value.concat(":00"));

  if(lock) {
    window.alert("Locked!")
    return;
  }

  if (!(from && to)) {
    window.alert("Select the dates");
    return;
  }

  lock = true;
  datatable.innerHTML = "";
  fetch(`http://localhost:8080/Data/getData?from=${from}&to=${to}`)
    .then((res) => {
      if (res.ok) return res.json();
      else {
        res.text().then((err) => window.alert(err));
        throw new Error("Response is not ok");
      }
    })
    .then((result) => {
      for (let res of result) {
        const row = datatable.insertRow();
        row.innerHTML = data_row(res);
      }
    })
    .then((res) => (lock = false))
    .catch((err) => {
      window.alert(err)
      lock = false
    });
}
