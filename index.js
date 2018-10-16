const fuse = require('fuse-bindings');
const meow = require('meow');
const AWS = require('aws-sdk');
const fs = require('fs');

const cli = meow(`
  Usage
    $ s3fuse bucket-name /mnt/path

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

const fds = {};
let fdCounter = 0;

function openFile(path) {
  fdCounter++;
  fds[fdCounter] = path;
  return fdCounter;
}

function getFdPath(fd) {
  return fds[fd];
}

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
      console.log('readdir result', allEntries);
      return cb(0, allEntries);
    } catch (err) {
      console.error(err);
      cb(fuse.EIO);
    }
  },

  getattr: async (path, cb) => {
    try {
      console.log('getattr(%s)', path)

      /* if (path === '/') {
        cb(0, getDirAttrs())
        return
      } */

      let cached = getCached(path);

      // Optimization to respond when asking for something we obviously don't have
      const match = path.match('(.+)/([^/]+)$');
      if (match) {
        const parentDir = match[1];
        let parentCached = getCached(parentDir);

        if (!parentCached) {
          console.log('getattr parent not cached, need to fetch', path);
          parentCached = await fetchPath(path);
        }

        const fileName = match[2];
        // console.log({ parentDir, fileName });
        // console.log('parentCached files:', parentCached.files, 'dirs:', parentCached.dirs);

        const dir = parentCached.subdirs.find(d => d === fileName);
        const file = parentCached.files.find(f => f.name === fileName);

        if (dir) {
          console.log('path is a directory', path);
          return cb(0, getDirAttrs());
        }

        if (file) {
          console.log('path is a file', path);
          return cb(0, getFileAttrs(file.size));
        }

        console.log('Not found in cached parent', path);
        return cb(fuse.ENOENT);
      }

      if (!cached) {
        console.log('getattr not cached, need to fetch', path);
        cached = await fetchPath(path);
      }

      // TODO we can remove this, no?
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
    try {
      if (flags & 3 !== 0) return cb(fuse.EIO);
      // TODO flags: only read supported, else return err
      console.log('open(%s, %d)', path, flags);
      const fd = openFile(path);
      cb(0, fd);
    } catch (err) {
      console.error(err);
      cb(fuse.EIO);
    }
  },

  read: async (path, fd, buf, len, pos, cb) => {
    try {
      console.log('read(%s, %d, %d, %d)', path, fd, len, pos)

      // const path = getFdPath(fd);
      // if (!path) return cb(fuse.EIO);

      const rangeFrom = pos;
      const rangeTo = pos + len;

      const resp = await s3.getObject({ Bucket: bucket, Key: path.replace(/^\//, ''), Range: `bytes=${rangeFrom}-${rangeTo}` }).promise();
      console.log('s3 get response:', resp);
      const respData = resp.Body;

      const respLen = Math.min(len, respData.length);
      if (respData.length > 0) respData.copy(buf, 0, 0, respLen);
      return cb(respLen);
    } catch (err) {
      console.error(err);
      cb(fuse.EIO);
    }
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
