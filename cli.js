const fuse = require('fuse-bindings');
const meow = require('meow');
const AWS = require('aws-sdk');
const fs = require('fs');
const memoize = require('memoizee');


// https://github.com/nodejs/node-v0.x-archive/issues/3045
// https://github.com/mafintosh/fuse-bindings

const cli = meow(`
  Usage
    $ s3fuse bucket-name /mnt/path

  Options
    --cache-timeout seconds  How many seconds until directory caches time out (default 60, 0 disables cache)
`);

const s3 = new AWS.S3();

const cacheTimeout = (cli.flags.cacheTimeout ? parseInt(cli.flags.cacheTimeout, 10) : 60) * 1000;
const bucket = cli.input[0];
const mountPath = cli.input[1];



// const fds = new Map();
let fdCounter = 0;

function openFile(path) {
  fdCounter++;
  // fds.set(fdCounter, path);
  return fdCounter;
}

function closeFile(fd) {
  // fds.delete(fd);
}

function getFdPath(fd) {
  return fds.get(fd);
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

async function fetchDir(path) {
  console.log('Fetching', path);

  const pathWithoutLeadingSlash = path.replace(/^\//, '');

  const resp = await s3.listObjectsV2({
    Bucket: bucket,
    Delimiter: '/',
    Prefix: path === '/' ? '' : `${pathWithoutLeadingSlash}/`,
  }).promise();
  // console.log('s3 response:', resp);

  const files = resp.Contents.map(c => ({
    size: c.Size,
    lastModified: c.LastModified,
    eTag: c.ETag,
    name: c.Key.split('/').pop(),
  }))
    .filter(f => f.name !== '');

  const subdirs = resp.CommonPrefixes.map(p => p.Prefix.replace(/\/$/, '').split('/').pop())
    .filter(d => d !== pathWithoutLeadingSlash);

  return {
    files,
    subdirs,
  };
}

const fetchDirMemoized = memoize(fetchDir, { primitive: true, promise: true, maxAge: cacheTimeout });

fuse.mount(mountPath, {
  readdir: async (path, cb) => {
    try {
      console.log('readdir(%s)', path)

      let dir = await fetchDirMemoized(path);

      const { files, subdirs } = dir;
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

      if (path === '/') {
        return cb(0, getDirAttrs())
      }

      // Optimization to respond when asking for something we obviously don't have
      const match = path.match('(.*)/([^/]+)$');
      if (match) {
        const parentDirPath = match[1] || '/';
        let parentDir = await fetchDirMemoized(parentDirPath);

        const fileName = match[2];
        // console.log({ parentDirPath, fileName });
        // console.log('parentDir files:', parentDir.files, 'subdirs:', parentDir.subdirs);

        const dir = parentDir.subdirs.find(d => d === fileName);
        const file = parentDir.files.find(f => f.name === fileName);

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

      return cb(fuse.ENOENT);
    } catch (err) {
      console.error(err);
      cb(fuse.EIO);
    }
  },

  open: (path, flags, cb) => {
    try {
      if (flags & 3 !== 0) return cb(fuse.EIO);

      console.log('open(%s, %d)', path, flags);
      const fd = openFile(path);
      cb(0, fd);
    } catch (err) {
      console.error(err);
      cb(fuse.EIO);
    }
  },

  release: (path, fd, cb) => {
    console.log('release(%s, %d)', path, fd);
    closeFile(fd);
    cb(0);
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
});


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
