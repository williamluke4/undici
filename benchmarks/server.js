'use strict'

const cluster = require('cluster')
const http = require('http')

if (cluster.isMaster) {
  for (let i = 0; i < 10; i++) {
    cluster.fork()
  }
} else {
  http.createServer((req, res) => {
    res.end('hello world')
  }).listen(3009)
}
