const _ = require('lodash')
const esClient = require('node-eventstore-client-temp')
const uuid = require('uuid')
const { Observable } = require('rxjs')
const fifo = require('fifo')
const EVENT_NUMBER = Symbol('EVENT_NUMBER')

const eventStore = (url) => {
  const consume = (topic, groupName, options) => {
    const opts = _.defaults(options, {
      bufferSize: 1,
      autoAck: false,
      retryOnError: true,
      retryInterval: 1000,
      encoding: 'json'
    })

    return Observable.create(observer => {
      var abort = false
      var timer = null
      var conn
      const happyPath = () => {
        if (abort) return
        if (conn) conn.close()
        conn = esClient.createConnection({}, url)
        conn.connect().then(() => {
          conn.connectToPersistentSubscription(
            topic,
            groupName,
            (ev, res) => {
              try {
                const data =
                  opts.encoding === 'utf8'
                  ? res.event.data.toString()
                  : opts.encoding === 'json'
                  ? JSON.parse(res.event.data.toString())
                  : res.event.data
                const recs = Array.isArray(data) ? data : [data]
                recs.forEach(rec => {
                  rec[EVENT_NUMBER] = res.event.eventNumber
                  observer.next({
                    eventNumber: res.event.eventNumber,
                    data: rec,
                    done: (err) => {
                      if (err) sadPath(err)
                      else {
                        try {
                          ev.acknowledge(res)
                        } catch (err) {
                          sadPath(err)
                        }
                      }
                    }
                  })
                })
              } catch (err) {
                sadPath(err)
              }
            },
            (sub, reason, error) => sadPath(`Dropped: ${reason || error}`),
            null, // User Credentials
            opts.bufferSize,
            opts.autoAck
          )
        }).catch(sadPath)
      }
      const sadPath = (err) => {
        if (opts.retryOnError) {
          console.warn(err.message || err)
          timer = setTimeout(happyPath, opts.retryInterval)
        } else {
          observer.error(err)
        }
      }
      happyPath()
      return () => {
        if (timer) clearTimeout(timer)
        abort = true
        conn.close()
      }
    })
  }
  const subscribe = (topic, options) => Observable.create(observer => {
    const opts = _.defaults(options, {
      retryOnError: true,
      retryInterval: 1000,
      encoding: 'json',
      offset: null
    })
    var abort = false
    var timer = null
    var conn
    const happyPath = () => {
      if (abort) return
      if (conn) conn.close()
      conn = esClient.createConnection({}, url)
      conn.connect().then(() => {
        conn.subscribeToStreamFrom(
          topic,
          opts.offset,
          true,
          (ev, res) => {
            try {
              const data =
                opts.encoding === 'utf8'
                ? res.event.data.toString()
                : opts.encoding === 'json'
                ? JSON.parse(res.event.data.toString())
                : res.event.data
              const recs = Array.isArray(data) ? data : [data]
              recs.forEach(rec => {
                rec[EVENT_NUMBER] = res.event.eventNumber
                observer.next(rec)
              })
            } catch (err) {
              sadPath(err)
            }
          },
          () => { /* console.log('Live!') */ },
          () => sadPath('Dropped'),
        )
      }).catch(sadPath)
    }
    const sadPath = (err) => {
      if (opts.retryOnError) {
        console.warn(err.message || err)
        timer = setTimeout(happyPath, opts.retryInterval)
      } else {
        observer.error(err)
      }
    }
    happyPath()
    return () => {
      if (timer) clearTimeout(timer)
      abort = true
      conn.close()
    }
  })

  const publish = (topic, stream) => Observable.create(observer => {
    const conn = esClient.createConnection({}, url)
    const queue = fifo()

    var connected = false
    var writing = false
    const doQueue = () => {
      if (writing) return
      if (!connected) return
      if (queue.length) {
        writing = true
        const data = queue.shift()
        if (data) {
          conn.appendToStream(
            topic,
            esClient.expectedVersion.any,
            esClient.createJsonEventData(uuid(), data)
          ).then(() => {
            writing = false
            observer.next(data)
            doQueue()
          }, err => observer.error(err))
        } else {
          observer.complete()
        }
      }
    }

    const push = (val) => {
      queue.push(val)
      doQueue()
    }

    const sub = stream.subscribe(
      val => push(val),
      err => observer.error(err),
      () => push(null)
    )

    conn.connect().then(() => {
      connected = true
      doQueue()
    }, (err) => observer.error(err))

    return () => {
      conn.close()
      sub.unsubscribe()
    }
  })

  return { subscribe, publish, consume }
}
Object.assign(eventStore, {
  EVENT_NUMBER
})
module.exports = eventStore
