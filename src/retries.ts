import { setTimeout } from "timers/promises";

export interface IRetriesOptions {
    initTime: number;
    maxAttempts: number;
    minTime?: number;
    maxTime?: number;
    backoffFactor?: number;
}

export const DEFAULT_RETRIES_OPTIONS = {
    initTime: 1000,
    maxAttempts: 1,
};

export async function withRetries<RT>(options: IRetriesOptions, func: () => RT, thisArg: any, ...args): Promise<RT> {
    const { initTime, minTime, maxTime, backoffFactor, maxAttempts } = options;

    let time = initTime;
    let attempts = 1;

    while (attempts <= maxAttempts) {
        try {
            return await func.apply(thisArg, args);
        } catch (error) {
            attempts++;
            if (attempts > maxAttempts) {
                throw error;
            }
        }
        await setTimeout(time);
        time *= backoffFactor;
        if (minTime && time < minTime) {
            time = minTime;
        } else if (maxTime && time > maxTime) {
            time = maxTime;
        }
    }
}