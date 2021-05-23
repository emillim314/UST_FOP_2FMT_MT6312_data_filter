const csv = require('csv-parser');
const csv_writer_creator = require('csv-writer').createObjectCsvWriter;
const fs = require('fs');

csv_writer = csv_writer_creator({
  path: 'data/filtered_data.csv',
  header: [{id:'control number', title:'control number'}]
});

const THRESHOLD = 2;

function checkvalid(values)
{
  let count = 0;
  for (let value in values)
  {
    if (!values[value])
    {
      ++count;
    }
  }
  return count < THRESHOLD;
}

function count_blank(values)
{
  let count = 0;
  for (let value in values)
  {
    if (!values[value])
    {
      ++count;
    }
  }
  return count;
}

function convert_data_json(data)
{
  let out = [];
  for (let datum of data)
  {
    out.push({'control number':datum});
  }
  return out;
}

function process(filename)
{
  return new Promise(
    (resolve, reject) => {
      let out = [];
      fs.createReadStream(filename)
        .pipe(csv())
        .on('data', (row) => {
          if (checkvalid(row))
          {
            out.push(parseInt(row['Control No.']));
          }
        })
        .on('end', () => {
          resolve(out);
        });
    }
  );
}

(async ()=>{
  let know_pre = await process('data/COVID-pre.csv');
  let know_post = await process('data/COVID-post.csv');
  let vax_pre = await process('data/vaccine-pre.csv');
  let vax_post = await process('data/vaccine-post.csv');
  let filtered = know_pre.filter(
    (control_id)=>{
      return know_post.indexOf(control_id) != -1;
    }
  ).filter(
    (control_id)=>{
      return vax_pre.indexOf(control_id) != -1;
    }
  ).filter(
    (control_id)=>{
      return vax_post.indexOf(control_id) != -1;
    }
  ).sort((a,b)=>{return a-b});
  console.log(`Filtered: ${filtered.length}`);
  for (let control_id of filtered)
  {
    console.log(control_id);
  }
  csv_writer.writeRecords(convert_data_json(filtered)).then(()=>{console.log('CSV file written.')});
})();
