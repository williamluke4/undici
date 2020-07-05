const {
  Readable,
  Duplex,
  PassThrough,
  finished
} = require('stream')
const {
  InvalidArgumentError,
  InvalidReturnValueError,
  RequestAbortedError
} = require('./errors')
const {
  kEnqueue,
  kResume
} = require('./symbols')
const ClientBase = require('./client-base')
const assert = require('assert')
const { AsyncResource } = require('async_hooks')

function nop () {}

class Client extends ClientBase {
  request (opts, callback) {
    if (callback === undefined) {
      return new Promise((resolve, reject) => {
        this.request(opts, (err, data) => {
          return err ? reject(err) : resolve(data)
        })
      })
    }

    if (typeof callback !== 'function') {
      throw new InvalidArgumentError('invalid callback')
    }

    if (!opts || typeof opts !== 'object') {
      process.nextTick(callback, new InvalidArgumentError('invalid opts'), null)
      return
    }

    const handler = new class extends AsyncResource {
      constructor(callback) {
        super('UNDICI_REQ')

        this.callback = callback
        this.body = null
      }
      onHeaders({
        statusCode,
        headers,
        opaque,
        resume
      }) {
        this.body = new Readable({
          autoDestroy: true,
          read: resume,
          destroy (err, callback) {
            if (!err && !this._readableState.endEmitted) {
              err = new RequestAbortedError()
            }
            if (err) {
              resume()
            }
            callback(err, null)
          }
        })
        this.body.destroy = this.runInAsyncScope.bind(this, this.body.destroy, this.body)

        this.callback(null, {
          statusCode,
          headers,
          opaque,
          body: this.body
        })
        this.callback = nop
      }
      onBody (chunk) {
        const ret = this.body.push(chunk)
        return this.body.destroyed ? null : ret
      }
      onError (err) {
        if (this.body) {
          if (!this.body.destroyed) {
            this.body.destroy(err)
          }
        } else {
          this.callback(err, null)
          this.callback = nop
        }
      }
    }(callback)

    this[kEnqueue](opts, handler)
  }

  pipeline (opts, factory) {
    if (!opts || typeof opts !== 'object') {
      return new PassThrough().destroy(new InvalidArgumentError('invalid opts'))
    }

    if (typeof factory !== 'function') {
      return new PassThrough().destroy(new InvalidArgumentError('invalid factory'))
    }

    // TODO: Move into handler.
    let body
    let res
    let req = new Readable({
      autoDestroy: true,
      read () {
        if (this[kResume]) {
          const resume = this[kResume]
          this[kResume] = null
          resume()
        }
      },
      destroy (err, callback) {
        if (err) {
          if (this[kResume]) {
            const resume = this[kResume]
            this[kResume] = null
            resume(err)
          } else if (!ret.destroyed) {
            // Stop ret from scheduling more writes.
            ret.destroy(err)
          }
        } else {
          assert(this._readableState.endEmitted)
          assert(!this[kResume])
        }

        callback(err)
      }
    })
    let ret = new Duplex({
      readableObjectMode: opts.objectMode,
      autoDestroy: true,
      read () {
        if (body && body.resume) {
          body.resume()
        }
      },
      write (chunk, encoding, callback) {
        assert(!req.destroyed)
        if (req.push(chunk, encoding)) {
          callback()
        } else {
          req[kResume] = callback
        }
      },
      final (callback) {
        req.push(null)
        callback()
      },
      destroy (err, callback) {
        if (!err && !this._readableState.endEmitted) {
          err = new RequestAbortedError()
        }
        if (!req.destroyed) {
          req.destroy(err)
        }
        if (res && !res.destroyed) {
          res.destroy(err)
        }
        callback(err)
      }
    })

    const handler = new class extends AsyncResource {
      constructor() {
        super('UNDICI_REQ')
        ret.destroy = this.runInAsyncScope.bind(this, ret.destroy, ret)
      }
      onHeaders({
        statusCode,
        headers,
        opaque,
        resume
      }) {
        res = new Readable({
          autoDestroy: true,
          read: resume,
          destroy (err, callback) {
            if (!err && !this._readableState.endEmitted) {
              err = new RequestAbortedError()
            }
            if (err) {
              if (!ret.destroyed) {
                ret.destroy(err)
              }
              resume()
            }
            callback(err, null)
          }
        })
        res.destroy = this.runInAsyncScope.bind(this, res.destroy, res)

        try {
          body = factory({
            statusCode,
            headers,
            opaque,
            body: res
          })
        } catch (err) {
          res.on('error', nop)
          if (!ret.destroyed) {
            ret.destroy(err)
          }
          return
        }

        // TODO: Should we allow !body?
        if (!body || typeof body.on !== 'function') {
          if (!ret.destroyed) {
            ret.destroy(new InvalidReturnValueError('expected Readable'))
          }
          return
        }

        // TODO: If body === res then avoid intermediate
        // and write directly to ret.push? Or should this
        // happen when body is null?

        // TODO: body.destroy?

        body
          .on('data', function (chunk) {
            if (!ret.push(chunk) && this.pause) {
              this.pause()
            }
          })
          .on('error', function (err) {
            if (!ret.destroyed) {
              ret.destroy(err)
            }
          })
          .on('end', function () {
            ret.push(null)
          })
          .on('close', function () {
            if (!this._readableState.endEmitted && !ret.destroyed) {
              ret.destroy(new RequestAbortedError())
            }
          })
      }
      onBody (chunk) {
        const ret = res.push(chunk)
        return res.destroyed ? null : ret
      }
      onError (err) {
        if (!ret.destroyed) {
          ret.destroy(err)
        }
      }
    }()

    // TODO: Avoid copy.
    opts = { ...opts, body: req }

    this[kEnqueue](opts, handler)

    return ret
  }

  stream (opts, factory, callback) {
    if (callback === undefined) {
      return new Promise((resolve, reject) => {
        this.stream(opts, factory, (err, data) => {
          return err ? reject(err) : resolve(data)
        })
      })
    }

    if (typeof callback !== 'function') {
      throw new InvalidArgumentError('invalid callback')
    }

    if (!opts || typeof opts !== 'object') {
      process.nextTick(callback, new InvalidArgumentError('invalid opts'), null)
      return
    }

    if (typeof factory !== 'function') {
      process.nextTick(callback, new InvalidArgumentError('invalid factory'), null)
      return
    }

    const handler = new class extends AsyncResource {
      constructor(callback) {
        super('UNDICI_REQ')
        this.body = null
        this.callback = callback
      }
      onHeaders({
        statusCode,
        headers,
        opaque,
        resume
      }) {
        let body
        try {
          body = factory({
            statusCode,
            headers,
            opaque
          })
        } catch (err) {
          this.callback(err, null)
          this.callback = nop
          return
        }

        if (!body) {
          this.callback(null, null)
          this.callback = nop
          return
        }

        if (
          typeof body.write !== 'function' ||
          typeof body.end !== 'function' ||
          typeof body.on !== 'function' ||
          typeof body.destroy !== 'function' ||
          typeof body.destroyed !== 'boolean'
        ) {
          this.callback(new InvalidReturnValueError('expected Writable'), null)
          this.callback = nop
          return
        }

        this.body = body
        this.body.destroy = this.runInAsyncScope.bind(this, this.body.destroy, this.body)
        this.body.on('drain', resume)
        // TODO: Avoid finished. It registers an unecessary amount of listeners.
        finished(this.body, { readable: false }, (err) => {
          this.body.removeListener('drain', resume)
          if (err) {
            if (!this.body.destroyed) {
              this.body.destroy(err)
            }
            resume()
          }
          this.callback(err, null)
          this.callback = nop
        })
      }
      onBody (chunk) {
        if (!this.body) {
          return null
        } else if (chunk == null) {
          this.body.end()
          return null
        } else {
          const ret = this.body.write(chunk)
          return this.body.destroyed ? null : ret
        }
      }
      onError (err) {
        if (this.body) {
          if (!this.body.destroyed) {
            this.body.destroy(err)
          }
        } else {
          this.callback(err, null)
          this.callback = nop
        }
      }
    }(callback)

    this[kEnqueue](opts, handler)
  }
}

module.exports = Client
