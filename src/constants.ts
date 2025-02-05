/**
 * Useful constants.
 */

import * as arrow from 'apache-arrow';


export const TIMESTAMP_TYPE_IDS = new Set(
    [
        arrow.Type.Timestamp,
        arrow.Type.TimestampMicrosecond,
        arrow.Type.TimestampMillisecond,
        arrow.Type.TimestampNanosecond,
        arrow.Type.TimestampSecond
    ]
);

export const DATE_TYPE_IDS = new Set(
    [
        arrow.Type.Date,
        arrow.Type.DateDay,
        arrow.Type.DateMillisecond,
    ]
);

export const DATE_TS_TYPE_IDS = new Set([...TIMESTAMP_TYPE_IDS, ...DATE_TYPE_IDS]);