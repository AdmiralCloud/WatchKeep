// Node modules
const fs = require('fs')
const path = require('path')

// Package modules
const zlib = require('zlib')
const split = require('split')
const async = require('async')
const _ = require('lodash')
const moment = require('moment')
const AWS = require('aws-sdk')


/**
 * Logs
 *
 * @param {Object} s3Config which contains s3 `key` & `secret`
 * @param {Object} logConfig which contains `bucket`, `prefix` & `format`
 * @param {Object} Custom logger
 * @return {Object}
 */
module.exports = function(s3Config, logConfig, log) {

  if( ! log) {
    log = {
      verbose: console.log,
      error: console.error,
      debug: console.debug,
      warn: console.debug
    }
  }

  if( ! s3Config) {
    log.error('s3Config is required')
    return false
  }

  if( ! logConfig) {
    log.error('logConfig is required')
    return false
  }

  AWS.config.update({
    "accessKeyId": s3Config.key,
    "secretAccessKey": s3Config.secret
  })

  let s3 = new AWS.S3()

  // Init
  let wk = {}
  let formats = require('./formats')
  let rootDir = process.cwd()
  let end = null
  let format  = formats[logConfig.format]

  let results = []

  /**
   * Download from S3, gunzip, and convert to JSON.
   *
   * @param {String} file path
   * @param {Object} runConfig object with configuration data
   * @param {String} runConfig.dailyLogPath -> path where to log daily, e.g. "./logs/"
   * @param {String} runConfig.moveProcessed -> path where to move processed files
   *
   * @param {Function} callback
   * @return void
   */
  wk.processFile = function(filePath, runConfig, cb) {
    if (filePath && filePath.length > 1) {
      log.verbose('Processing', filePath)


      // store all files to daily files on the uploadserver -> we can save them later to s3 or where ever
      // EX: cloudfront.mediadev.admiralcloud.com/E32Q8HEC4MZ488.2015-06-09-16.d96dbed0.gz
      let fileName = filePath.split('/') && _.last(filePath.split('/'));
      if (!fileName) return cb('error_cannotLogToDailyLogFile');

      let date = fileName.split('.') && fileName.split('.')[1];
      let day = date.substr(0,10);
      let dailyLogPath = runConfig.dailyLogPath || './logs/';
      let daily = dailyLogPath + day+'_daily.log';


      let params = {
        Bucket: logConfig.bucket,
        Key: filePath
      }

      async.series({
        headRequest: function(done) {
          s3.headObject(params, done);
        },
        checkOrCreateFile: function(done) {
          fs.exists(daily, function(exists) {
            if (exists) return done();
            fs.writeFile(daily, '', done);
          });
        }
      }, function allChecked(err) {
        if (err) {
          log.error("W96",err);
          return cb(err);
        }

        let read = s3.getObject(params).createReadStream()
        let gunzip = zlib.createGunzip()

        let reader = read

        if (format.gzip) {
          read.pipe(gunzip)
          reader = gunzip
        }

        let json = reader.pipe(split())
        // split() makes each line a chunk
        json.on('data', function(row) {
          fs.appendFile(daily, row+'\n', function(err) {
            if (err) log.error("W116",err);
          });
          let data = format.toJson(row)
          if(data) results.push(data)
        });

        json.on('error', cb);

        json.on('end', function() {
          if (runConfig.moveProcessed) {
            // move processed file to a new location
            let moveProcessed = runConfig.moveProcessed;
            if (runConfig.moveProcessedSuffix === 'YM') {
              // use YYYY-MM path from the original path/key - xxx.2016-11-01-07.xxx.gz
              const regex = /(\w{4})-(\w{2})-(\w{2})/
              let date = filePath.match(regex)
              if (date && date.length > 0) {
                // 2016-11-01
                moveProcessed += date[1] + date[2];
              }
            }
            let copyParams = {
              Bucket: logConfig.bucket, // required
              CopySource: logConfig.bucket+'/'+filePath, // required
              Key: moveProcessed+'/'+filePath // required
            }
            s3.copyObject(copyParams, function(err) {
              if (err) {
                log.error("W131", copyParams);
                log.error("W132",err);
                return cb(err);
              } // an error occurred
              else {
                // delete original file
                let deleteParams = {
                  Bucket: logConfig.bucket, // required
                  Key: filePath
                };
                s3.deleteObject(deleteParams, function(err) {
                  if (err) {
                    log.error("W141", deleteParams);
                    log.error("W142", err);
                  } // an error occurred
                  else log.debug(filePath," moved");
                  return cb(err);
                });
              }
            });
          }
        });

      });

    }
  }

  /**
   * List files in bucket that match prefix.
   *
   * @param {Object} params
   * @param {Function} callback
   * @return void
   */
  wk.listFiles = function(params, cb) {
    s3.listObjects(params, function(err, data) {
      if (err) return log.error(err)

      // Get list of file paths.
      let paths = []
      data.Contents.forEach(function(filePath) {
        if (filePath.Size > 0) paths.push(filePath.Key)
      })

      // Iterate to the next batch.
      // s3.listObjects is limited to 1000 keys.
      // This requests the next batch
      // @todo Fix truncation
      // var lastPath = paths[paths.length - 1]
      if (data.IsTruncated) {
        log.warn('listObjects has been truncated. You are not getting all objects.')
        return cb(paths)
        // params.Marker = lastPath
        // var endMarker = logConfig.prefix + end.format(format.fileDateFormat)
        // if (lastPath < endMarker) {
        //   wk.listFiles(params, function(morePaths){
        //     paths = paths.concat(morePaths)
        //     cb(paths)
        //   })
        // } else {
        //   cb(paths)
        // }
      } else {
        return cb(paths)
      }
    })
  }

  /**
   * Run
   *
   * @param {Object} runConfig > moveProcessed: path to move processed files to (to avoid duplicates)
   * @param {Array} paths to exclude
   * @param {Function} callback
   * @return void
   */
  wk.run = function(runConfig, callback) {

    let listParams = {
      Bucket: logConfig.bucket,
      MaxKeys: logConfig.maxKeys || 1000,
      Prefix: logConfig.prefix
    }

    wk.listFiles(listParams, function(paths) {
      // Exclude paths that have already been fetch
      if(runConfig.exclude) {
        log.verbose('%d paths before exclude', paths.length)
        log.verbose('%d paths in exclude list', runConfig.exclude.length)
        let before = paths.length
        // Exclude paths where they match
        paths = _.filter(paths, function(path) {
          return ! _.contains(runConfig.exclude, path)
        })
        log.debug('%d paths to be processed', paths.length)
        log.debug('%d paths have been excluded', before - paths.length)
      }

      // Process each file path.
      async.eachSeries(paths, function(path, itDone) {
        wk.processFile(path, runConfig, function(err, result) {
          if (err) return itDone(err);
          results.push(result);
          return itDone();
        });
      }, function(err) {
          return callback(err, results, paths)
      });
    })
  }

  return wk
}
