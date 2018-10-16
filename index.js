const fuse = require('fuse-bindings');
const meow = require('meow');
const AWS = require('aws-sdk');
const fs = require('fs');

const cli = meow(`
  Usage
    $ s3fuse bucket /mnt/path

  Options
`);


// https://github.com/nodejs/node-v0.x-archive/issues/3045
// https://github.com/mafintosh/fuse-bindings

const s3 = new AWS.S3();

// cli.flags.x
const bucket = cli.input[0];
const mountPath = cli.input[1];

// const attrs = {};

// TODO use map instead
// TODO cleanup after some time to prevent overgrowing
// TODO invalidate cache logic
const globalCache = {};

function getCached(path) {
  return globalCache[path];
}

const getDirAttrs = () => ({
  mtime: new Date(),
  atime: new Date(),
  ctime: new Date(),
  nlink: 1,
  size: 100,
  mode: 16877,
  uid: process.getuid ? process.getuid() : 0,
  gid: process.getgid ? process.getgid() : 0,
});

const getFileAttrs = (size) => ({
  mtime: new Date(),
  atime: new Date(),
  ctime: new Date(),
  nlink: 1,
  size,
  mode: 33188,
  uid: process.getuid ? process.getuid() : 0,
  gid: process.getgid ? process.getgid() : 0,
});

async function fetchPath(path) {
  const resp = await s3.listObjectsV2({ Bucket: bucket, Delimiter: '/', Prefix: path === '/' ? '' : `${path.replace(/^\//, '')}/` }).promise();
  console.log('s3 response:', resp);

  // TODO IsTruncated

  const files = resp.Contents.map(c => ({
    size: c.Size,
    lastModified: c.LastModified,
    eTag: c.ETag,
    name: c.Key.split('/').pop(),
  }))
    .filter(f => f.name !== '');

  const parsed = {
    files: files,
    subdirs: resp.CommonPrefixes.map(p => p.Prefix.replace(/\/$/, '').split('/').pop()),
  };
  console.log('parsed', parsed);

  globalCache[path] = parsed;

  return parsed;
}

fuse.mount(mountPath, {
  readdir: async (path, cb) => {
    try {
      console.log('readdir(%s)', path)

      let cached = getCached(path);

      if (!cached) {
        console.log('readdir not cached, need to fetch', path);
        cached = await fetchPath(path);
      }

      const { files, subdirs } = cached;
      const allEntries = [...files.map(f => f.name), ...subdirs];
      return cb(0, allEntries);
    } catch (err) {
      console.error(err);
      cb(fuse.EIO);
    }
  },
  getattr: async (path, cb) => {
    try {
      console.log('getattr(%s)', path)

      if (path === '/') {
        cb(0, getDirAttrs())
        return
      }

      let cached = getCached(path);

      // Optimization to respond when asking for something we obviously don't have
      const match = path.match('(.+)/([^/]+)$');
      if (match) {
        const parentDir = match[1];
        const parentCached = getCached(parentDir);
        if (parentCached) {
          const fileName = match[2];
          console.log({ parentDir, fileName });
          console.log({ parentCached });
          if (![...parentCached.files.map(f => f.name), ...parentCached.subdirs].includes(fileName)) {
            console.log('Not found in cached parent', path);
            return cb(fuse.ENOENT);
          }
        }
      }

      if (!cached) {
        console.log('getattr not cached, need to fetch', path);
        cached = await fetchPath(path);
      }

      if (cached.files.length > 1 || cached.subdirs.length > 0) {
        console.log('path is a directory', path);
        return cb(0, getDirAttrs());
      } else if (cached.files.length === 1) {
        console.log('path is a file', path);
        return cb(0, getFileAttrs(12)); // TODO size
      }

      return cb(fuse.ENOENT);
    } catch (err) {
      console.error(err);
      cb(fuse.EIO);
    }
  },

  open: function (path, flags, cb) {
    console.log('open(%s, %d)', path, flags)
    cb(0, 42) // 42 is an fd
  },
  read: function (path, fd, buf, len, pos, cb) {
    console.log('read(%s, %d, %d, %d)', path, fd, len, pos)
    var str = 'hello world\n'.slice(pos, pos + len)
    if (!str) return cb(0)
    buf.write(str)
    return cb(str.length)
  }
}, function (err) {
  if (err) throw err
  console.log('filesystem mounted on ' + mountPath)
})

let sigIntReceived = false;

process.on('SIGINT', () => {
  console.log('SIGINT');
  if (sigIntReceived) process.exit(1);
  sigIntReceived = true;

  fuse.unmount(mountPath, function (err) {
    if (err) {
      console.error('filesystem at ' + mountPath + ' unmount error', err);
      process.exit(1);
    } else {
      console.log('filesystem at ' + mountPath + ' unmounted')
    }
  })
});
