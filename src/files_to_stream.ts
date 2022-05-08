import { PassThrough } from "stream";
import { createReadStream } from "fs";

import type { ReadStream } from 'fs';
import type { TransformOptions } from 'stream';


interface ReadStreamOptions {
    highWaterMark?: number;
}

export interface FilesToStreamOptions {
    passThrough?: TransformOptions;
    readStream?: ReadStreamOptions;
}

function addFilesToStream(passThrough: PassThrough, files: string[], options: ReadStreamOptions): ReadStream {
    const file = files.shift();
    const stream = createReadStream(file, options);
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
