'use strict'
const { Writable } = require('stream')
const http = require('http')
const Benchmark = require('benchmark')
const undici = require('..')

// # Start the h2o server (in h2o repository)
// # Then change the port below to 8080
// h2o -c examples/h2o/h2o.conf
//
// # Alternatively start the Node.js server
// node benchmarks/server.js
//
// # Start the benchmarks
// node benchmarks/index.js

const httpOptions = {
  protocol: 'http:',
  hostname: 'localhost',
  method: 'GET',
  path: '/',
  port: 3009,
  agent: new http.Agent({
    keepAlive: true,
    maxSockets: 100
  })
}

const undiciOptions = {
  path: '/',
  method: 'GET',
  requestTimeout: 0
}

const PIPELINING = 10
const pool = undici(`http://${httpOptions.hostname}:${httpOptions.port}`, {
  connections: 100,
  pipelining: PIPELINING
})

const suite = new Benchmark.Suite()

Benchmark.options.minSamples = 200

suite
  // .add('http - keepalive', {
  //   defer: true,
  //   fn: deferred => {
  //     let k = PIPELINING
  //     for (let n = 0; n < k; ++n) {
  //       http.get(httpOptions, response => {
  //         const stream = new Writable({
  //           write (chunk, encoding, callback) {
  //             callback()
  //           }
  //         })
  //         stream.once('finish', () => {
  //           if (--k === 0) {
  //             deferred.resolve()
  //           }
  //         })

  //         response.pipe(stream)
  //       })
  //     }
  //   }
  // })
  // .add('undici - pipeline', {
  //   defer: true,
  //   fn: deferred => {
  //     let k = PIPELINING
  //     for (let n = 0; n < k; ++n) {
  //       pool
  //         .pipeline(undiciOptions, data => {
  //           return data.body
  //         })
  //         .end()
  //         .pipe(new Writable({
  //           write (chunk, encoding, callback) {
  //             callback()
  //           }
  //         }))
  //         .once('finish', () => {
  //           if (--k === 0) {
  //             deferred.resolve()
  //           }
  //         })
  //     }
  //   }
  // })
  // .add('undici - request', {
  //   defer: true,
  //   fn: deferred => {
  //     let k = PIPELINING
  //     for (let n = 0; n < k; ++n) {
  //       pool.request(undiciOptions, (error, { body }) => {
  //         if (error) {
  //           throw error
  //         }

  //         const stream = new Writable({
  //           write (chunk, encoding, callback) {
  //             callback()
  //           }
  //         })
  //         stream.once('finish', () => {
  //           if (--k === 0) {
  //             deferred.resolve()
  //           }
  //         })

  //         body.pipe(stream)
  //       })
  //     }
  //   }
  // })
  .add('undici - stream', {
    defer: true,
    fn: deferred => {
      let k = PIPELINING
      for (let n = 0; n < k; ++n) {
        pool.stream(undiciOptions, () => {
          const stream = new Writable({
            write (chunk, encoding, callback) {
              callback()
            }
          })
          stream.once('finish', () => {
            if (--k === 0) {
              deferred.resolve()
            }
          })

          return stream
        }, error => {
          if (error) {
            throw error
          }
        })
      }
    }
  })
  // .add('undici - dispatch', {
  //   defer: true,
  //   fn: deferred => {
  //     let k = PIPELINING
  //     for (let n = 0; n < k; ++n) {
  //       const stream = new Writable({
  //         write (chunk, encoding, callback) {
  //           callback()
  //         }
  //       })
  //       stream.once('finish', () => {
  //         if (--k === 0) {
  //           deferred.resolve()
  //         }
  //       })
  //       pool.dispatch(undiciOptions, new SimpleRequest(stream))
  //     }
  //   }
  // })
  // .add('undici - noop', {
  //   defer: true,
  //   fn: deferred => {
  //     for (let n = 0; n < PIPELINING - 1; ++n) {
  //       pool.dispatch(undiciOptions, new NoopRequest())
  //     }
  //     pool.dispatch(undiciOptions, new NoopRequest(deferred))
  //   }
  // })
  .on('cycle', event => {
    console.log(String(event.target))
  })
  .run()

class NoopRequest {
  constructor (deferred) {
    this.deferred = deferred
  }

  onConnect (abort) {

  }

  onHeaders (statusCode, headers, resume) {

  }

  onData (chunk) {
    return true
  }

  onComplete (trailers) {
    if (this.deferred) {
      this.deferred.resolve()
    }
  }
}

class SimpleRequest {
  constructor (dst) {
    this.dst = dst
  }

  onConnect (abort) {
  }

  onHeaders (statusCode, headers, resume) {
    this.dst.on('drain', resume)
  }

  onData (chunk) {
    return this.dst.write(chunk)
  }

  onComplete () {
    this.dst.end()
  }
}
