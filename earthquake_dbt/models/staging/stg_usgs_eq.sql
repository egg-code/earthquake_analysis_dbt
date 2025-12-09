with source as (
    SELECT * FROM {{ source('raw', 'usgs_raw') }}
),
renamed as (
    SELECT
        id AS event_id,
        mag AS magnitude,
        mag_type AS magnitude_type,
        place AS location,
        time AS event_occurred_timestamp,
        updated AS updated_timestamp,
        tz AS timezone,
        url AS event_url,
        detail AS detail_url,
        felt AS felt_reports,
        cdi,
        mmi,
        alert,
        status,
        tsunami,
        sig,
        net,
        code,
        ids,
        sources,
        types,
        nst,
        dmin,
        rms,
        gap,
        eq_type
        title,
        longitude,
        latitude,
        depth,
        ingestion_time
    FROM source
    where mag is not NULL
)
SELECT * FROM renamed