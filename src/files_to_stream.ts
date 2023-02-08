import { PassThrough } from "stream";
import { createReadStream } from "fs";

import type { TransformOptions, Readable } from 'stream';

import { CompressionFormat, getDecoder } from "./encoder_decoder";

interface ReadStreamOptions {
    highWaterMark?: number;
}

export interface FilesToStreamOptions {
    passThrough?: TransformOptions;
    readStream?: ReadStreamOptions;
}

function addFilesToStream(passThrough: PassThrough, files: string[], options: ReadStreamOptions): Readable {
    const file = files.shift();
    const fileExtension = file.split('.').pop();

    let stream: Readable = createReadStream(file, options);

    const decoder = getDecoder(fileExtension as CompressionFormat);

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
