/**
 * Minimal wrapper around Node's `worker_threads`. Make a thread pool with the
 * path to a JS file and a number of workers, then call `pool.send(*args)`.
 * You'll get back a promise for the result.
 *
 * In the worker file, call `WorkerPool.implementWorker((x, y) => z)` to
 * respond to calls through the pool, where `(x, y) => z` is a synchronous
 * function that returns a result. It can take any number of arguments, and
 * exceptions will be caught and re-thrown in the calling process.
 */

const { Worker, parentPort } = require('node:worker_threads');

class SimpleWorker {
  constructor (sourcePath) {
    this._workerSource = sourcePath;
    this._worker = this._createWorker();
    this._promise = null;
    this._timer = null;
  }

  send (options, ...args) {
    return new Promise((resolve, reject) => {
      this._promise = { resolve, reject };
      this._worker.postMessage({id: Math.random(), args});
      if (options && options.timeout) {
        this._timer = setTimeout(() => this._handleTimeout(), options.timeout);
      }
    });
  }

  terminate (cause = null) {
    // NOTE: terminate() returns a promise; it *may* be prudent to wait for it
    // before regenerating a new worker.
    console.time('Terminating')
    const termination = this._worker.terminate()
      .then(() => console.log('Terminated'))
      .catch(() => console.error('Failed to terminate'))
      .finally(() => console.timeEnd('Terminating'));
    this._worker = this._createWorker();
    if (this._promise) {
      this._handleMessage({error: cause || 'TERMINATED'});
    }
    return termination;
  }

  _createWorker () {
    const worker = new Worker(this._workerSource);
    worker.on('message', this._handleMessage.bind(this));
    return worker;
  }

  _handleTimeout () {
    this.terminate('TIMEDOUT');
  }

  _handleMessage (message) {
    // TODO: should look up with ID
    if (this._promise) {
      clearTimeout(this._timer);
      const promise = this._promise;
      this._promise = null;

      if (message.error) {
        const error = new Error(message.error);
        if (error.stack) error.stack = message.stack;
        promise.reject(error);
      }
      else {
        promise.resolve(message.value);
      }
    }
    else {
      console.warn('Got callback from worker with no registered handler!', message);
    }
  }
}

class WorkerPool {
  constructor (sourcePath, size) {
    this._free = [];
    this._waiting = [];

    for (let i = size; i > 0; i--) {
      this._free.push(new SimpleWorker(sourcePath));
    }
  }

  async send (options, ...args) {
    const worker = await this.acquire();
    let result;
    try {
      result = await worker.send(options, ...args);
    }
    finally {
      this.release(worker);
    }
    return result;
  }

  acquire () {
    return new Promise((resolve) => {
      let worker = this._free.pop();
      if (worker) return resolve(worker);

      this._waiting.push((worker) => {
        resolve(worker);
      });
    });
  }

  release (worker) {
    const waiting = this._waiting.shift();
    if (waiting) {
      waiting(worker);
    }
    else {
      this._free.push(worker);
    }
  }

  static implementWorker (implementation) {
    parentPort.on('message', ({id, args}) => {
      try {
        const value = implementation(...args);
        parentPort.postMessage({id, value});
      }
      catch (error) {
        parentPort.postMessage({id, error: error.message, stack: error.stack});
      }
    });
  }
}

WorkerPool.SimpleWorker = SimpleWorker;

module.exports = WorkerPool;
