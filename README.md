# s3fuse 📁☁️
`s3fuse` is yet another S3 FUSE file system implementation, this time in Node.js! It aims to be a super simple, stable and relatively fast way of browsing S3 and downloading files. Writing can be risky because of eventual consistency, and will not be supported by this project.

I wrote this quickly because I needed a really simple and stable S3 file system for running on a server application. Other options were either too complex, buggy or crashing/hanging all the time.

## Install

1. Install Fuse
2. Install Node.js
3. Run `npm install -g s3fuse`

## Usage

Specify AWS settings and credentials in your `~/.aws/` or in the environment:

```
export AWS_PROFILE=profile
export AWS_ACCESS_KEY_ID=key
export AWS_SECRET_ACCESS_KEY=secretkey
```

Then run:

```
s3fuse bucket-name /mnt/path
```

## Features
- Caches dir and file names for faster browsing
- Simple implementation and should be very stable

## Limitations
- Read only
- Only sequential requests (no parallelization yet in [node-fuse-bidings](https://github.com/mafintosh/fuse-bindings/issues/9))
- Downloading files is very slow because of previous point

## TODO
- Handle objects with no permissions (currently will probably just give Input/output error)
- Automated tests
- Maybe use [libfuse](https://github.com/libfuse/libfuse) directly using [node-ffi](https://github.com/node-ffi/node-ffi)
- Handle IsTruncated

## Related
- https://github.com/s3fs-fuse/s3fs-fuse
