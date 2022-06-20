import { PassThrough } from "stream";
import { createReadStream } from "fs";
import { createBrotliDecompress, createGunzip, createInflate } from "zlib";

import type { TransformOptions, Readable } from 'stream';

interface ReadStreamOptions {
    highWaterMark?: number;
}

export interface FilesToStreamOptions {
    passThrough?: TransformOptions;
    readStream?: ReadStreamOptions;
}

function getStreamDecoder(fileExtension: 'gz' | 'br' | 'deflate' | string) {
    switch (fileExtension) {
        case 'gz':
            return createGunzip();
        case "br":
            return createBrotliDecompress();
        case "deflate":
            return createInflate();
    }
}

function addFilesToStream(passThrough: PassThrough, files: string[], options: ReadStreamOptions): Readable {
    const file = files.shift();
    const fileExtension = file.split('.').pop();

    let stream: Readable = createReadStream(file, options);

    const decoder = getStreamDecoder(fileExtension);
    if (decoder) {
        stream = stream.pipe(decoder);
    }

    if (files.length > 0) {
        stream.pipe(passThrough, { end: false });
        stream.once('end', function () {
            addFilesToStream(passThrough, files, options);
        });
    } else {
        stream.pipe(passThrough);
    }
    return stream;
}

export function filesToStream(files: string[], options?: FilesToStreamOptions): PassThrough {
    const passThroughOptions = Object.assign({
        readableHighWaterMark: 65536,
        writableHighWaterMark: 65536
    }, options?.passThrough);

    const readStreamOptions = Object.assign({}, options?.readStream);

    const passThrough = new PassThrough(passThroughOptions);

    if (files.length > 0) {
        addFilesToStream(passThrough, files, readStreamOptions);
    }

    return passThrough;
}
