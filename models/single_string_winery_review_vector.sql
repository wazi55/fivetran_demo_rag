{{ config(materialized='table') }}
with stg as  (
    SELECT
        CONCAT(
            'This winery name is ',
            IFNULL(WINERY_OR_VINEYARD, ' Name is not known'),
            '. California wine region: ',
            IFNULL(CA_WINE_REGION, 'unknown'),
            '',
            ' The AVA Appellation is the ',
            IFNULL(AVA_APPELLATION_SUB_APPELLATION, 'unknown'),
            '.',
            ' The website associated with the winery is ',
            IFNULL(WEBSITE, 'unknown'),
            '.',
            ' Price Range: ',
            IFNULL(PRICE_RANGE, 'unknown'),
            '.',
            ' Tasting Room Hours: ',
            IFNULL(TASTING_ROOM_HOURS, 'unknown'),
            '.',
            ' Are Reservations Required or Not: ',
            IFNULL(RESERVATION_REQUIRED, 'unknown'),
            '.',
            ' Winery Description: ',
            IFNULL(WINERY_DESCRIPTION, 'unknown'),
            '',
            ' The Primary Varietals this winery offers: ',
            IFNULL(PRIMARY_VARIETALS, 'unknown'),
            '.',
            ' Thoughts on the Tasting Room Experience: ',
            IFNULL(TASTING_ROOM_EXPERIENCE, 'unknown'),
            '.',
            ' Amenities: ',
            IFNULL(AMENITIES, 'unknown'),
            '.',
            ' Awards and Accolades: ',
            IFNULL(AWARDS_AND_ACCOLADES, 'unknown'),
            '.',
            ' Distance Travel Time considerations: ',
            IFNULL(DISTANCE_AND_TRAVEL_TIME, 'unknown'),
            '.',
            ' User Rating: ',
            IFNULL(USER_RATING, 'unknown'),
            '.',
            ' The Secondary Varietals for this winery: ',
            IFNULL(SECONDARY_VARIETALS, 'unknown'),
            '.',
            ' Wine Styles: ',
            IFNULL(WINE_STYLES, 'unknown'),
            '.',
            ' Events and Activities: ',
            IFNULL(EVENTS_AND_ACTIVITIES, 'unknown'),
            '.',
            ' Sustainability Practices: ',
            IFNULL(SUSTAINABILITY_PRACTICES, 'unknown'),
            '.',
            ' Social Media Channels: ',
            IFNULL(SOCIAL_MEDIA, 'unknown'),
            '',
            ' Address: ',
            IFNULL(ADDRESS, 'unknown'),
            '',
            ' City: ',
            IFNULL(CITY, 'unknown'),
            '',
            ' State: ',
            IFNULL(STATE, 'unknown'),
            '',
            ' ZIP: ',
            IFNULL(ZIP, 'unknown'),
            '',
            ' Phone: ',
            IFNULL(PHONE, 'unknown'),
            '',
            ' Winemaker: ',
            IFNULL(WINEMAKER, 'unknown'),
            '',
            ' Did Kelly Kohlleffel recommend this winery?: ',
            IFNULL(KELLY_KOHLLEFFEL_RECOMMENDED, 'unknown'),
            ''
        ) AS winery_information
    FROM
        `'{{ env_var("PROJECT") }}'.'{{ env_var("DATASET") }}'.'{{ env_var("TABLE") }}'`
) 
SELECT
    *
FROM
    ML.GENERATE_TEXT_EMBEDDING(
        MODEL `project_beyondsql_genai_textembedding.llm_embedding_model`,
        (
            select
                winery_information as content
            from
                stg
        ),
        STRUCT(TRUE AS flatten_json_output)
    )