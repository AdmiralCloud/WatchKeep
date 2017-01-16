// Cloudfront
//
const header = [
  'date',
  'time',
  'x-edge-location',
  'sc-bytes',
  'c-ip',
  'cs-method',
  'cs(Host)',
  'cs-uri-stem',
  'sc-status',
  'cs(Referer)',
  'cs(User-Agent)',
  'cs-uri-query',
  'cs(Cookie)',
  'x-edge-result-type',
  'x-edge-request-id',
  'x-host-header',
  'cs-protocol',
  'cs-bytes',
  'time-taken',
  'x-forwarded-for',
  'ssl-protocol',
  'ssl-cipher',
  'x-edge-response-result-type',
  'cs-protocol-version'
];

// Convert log format into a JSON object.
//
const convert = function(row) {
  // Skip commented lines.
  if (row[0] === '#') return null;
  // Skip blank lines.
  if (row === '') return null;

  let obj = {};
  let vals = row.split('\t');
  let fields = header.length;

  if (vals.length >= fields) {
    header.forEach(function(key, index){
      obj[key] = vals[index];
    });
    
    return obj
  } else {
    console.log("Skipping row. Values don't match expected length.")
  }

  return null;
};

module.exports = {
  toJson: convert,
  gzip: true,
  fileDateFormat: 'YYYY-MM-DD',
  reportFields: header
};
