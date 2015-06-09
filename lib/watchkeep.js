// Node modules
var fs = require('fs')
var path = require('path')

// Package modules
var zlib = require('zlib')
var split = require('split')
var async = require('async')
var moment = require('moment')
var AWS = require('aws-sdk')


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

  var s3 = new AWS.S3()

  // Init
  var wk = {}
  var formats = require('./formats')
  var rootDir = process.cwd()
  var end = null
  var format  = formats[logConfig.format]

  var results = []

  /**
   * Download from S3, gunzip, and convert to JSON.
   *
   * @param {String} file path
   * @param {Function} callback
   * @return void
   */
  wk.processFile = function(filePath, runConfig, cb) {
    if (filePath && filePath.length > 1) {
      log.verbose('Processing', filePath)


      // store all files to daily files on the uploadserver -> we can save them later to s3 or where ever
      // EX: cloudfront.mediadev.admiralcloud.com/E32Q8HEC4MZ488.2015-06-09-16.d96dbed0.gz
      var fileName = filePath.split('/') && _.last(filePath.split('/'));
      if (!fileName) return cb('error_cannotLogToDailyLogFile');
      var date = fileName.split('.') && fileName.split('.')[1];
      var day = date.substr(0,10);
      var daily = './log/'+day+'_daily.log';


      var params = {
        Bucket: logConfig.bucket,
        Key: filePath
      }

      async.series({
        headRequest: function(done) {
          s3.headObject(params, function(err) {
            return done(err);
          });
        },
        putToDailyLogFiles: function(done) {
          fs.exists(daily, function(exists) {
            if (exists) return done();
            fs.writeFile(daily, '', function(err) {
              return done(err);
            });
          });
        }
      }, function allChecked(err) {
        if (err) {
          log.error(err);
          return cb(err);
        }

        var read = s3.getObject(params).createReadStream()
        var gunzip = zlib.createGunzip()

        var reader = read

        if (format.gzip) {
          read.pipe(gunzip)
          reader = gunzip
        }

        var json = reader.pipe(split())
        // split() makes each line a chunk
        json.on('data', function(row) {
          fs.appendFile(daily, row+'\n', function(err) {
            if (err) log.error(116,err);
          });
          var data = format.toJson(row)
          if(data) results.push(data)
        });

        json.on('error', function(err) {
          cb(err)
        })

        json.on('end', function() {
          if (runConfig.moveProcessed) {
            // move processed file to a new location
            var copyParams = {
              Bucket: logConfig.bucket, // required
              CopySource: logConfig.bucket+'/'+filePath, // required
              Key: runConfig.moveProcessed+'/'+filePath // required
            }
            s3.copyObject(copyParams, function(err) {
              if (err) log.error(err, err.stack); // an error occurred
              else {
                // delete original file
                var deleteParams = {
                  Bucket: logConfig.bucket, // required
                  Key: filePath
                };
                s3.deleteObject(deleteParams, function(err) {
                  if (err) log.error(err, err.stack); // an error occurred
                  else log.debug(filePath," moved");
                });
              }
            });
          }
          cb()
        })

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
      var paths = []
      data.Contents.forEach(function(filePath) {
        paths.push(filePath.Key)
      })

      // Iterate to the next batch.
      // s3.listObjects is limited to 1000 keys.
      // This requests the next batch
      // @todo Fix truncation
      // var lastPath = paths[paths.length - 1]
      if (data.IsTruncated) {
        log.warn('listObjects has been truncated. You are not getting all objects.')
        cb(paths)
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
        cb(paths)
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

    var listParams = {
      Bucket: logConfig.bucket,
      MaxKeys: logConfig.maxKeys || 1000,
      Prefix: logConfig.prefix
    }

    wk.listFiles(listParams, function(paths) {
      // Exclude paths that have already been fetch
      if(runConfig.exclude) {
        log.verbose('%d paths before exclude', paths.length)
        log.verbose('%d paths in exclude list', runConfig.exclude.length)
        var before = paths.length
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
          if (err) callback(err)
          return callback(null, results, paths)
      });
    })
  }

  return wk
}
