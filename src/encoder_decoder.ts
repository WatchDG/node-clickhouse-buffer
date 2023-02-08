import {
    createBrotliCompress,
    createBrotliDecompress,
    createDeflate,
    createGunzip,
    createGzip,
    createInflate
} from "zlib";

export enum CompressionFormat {
    GZIP = 'gzip',
    BROTLI = 'br',
    DEFLATE = 'deflate',
}

export function getEncoder(compressionFormat?: CompressionFormat) {
    switch (compressionFormat) {
        case CompressionFormat.GZIP:
            return createGzip();
        case CompressionFormat.BROTLI:
            return createBrotliCompress();
        case CompressionFormat.DEFLATE:
            return createDeflate();
    }
}

export function getDecoder(compressionFormat?: CompressionFormat) {
    switch (compressionFormat) {
        case CompressionFormat.GZIP:
            return createGunzip();
        case CompressionFormat.BROTLI:
            return createBrotliDecompress();
        case CompressionFormat.DEFLATE:
            return createInflate();
    }
}